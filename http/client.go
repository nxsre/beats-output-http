package http

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/dgrijalva/jwt-go/v4"
	"github.com/jinzhu/copier"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/outil"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/elastic/elastic-agent-libs/mapstr"
	"github.com/elastic/elastic-agent-libs/transport"
	"github.com/elastic/elastic-agent-libs/transport/tlscommon"
)

// Client struct
type Client struct {
	Connection
	tlsConfig *tlscommon.TLSConfig
	params    map[string]string
	// additional configs
	compressionLevel int
	proxyURL         *url.URL
	batchPublish     bool
	observer         outputs.Observer
	headers          map[string]string
	format           string
}

// ClientSettings struct
type ClientSettings struct {
	URL                string
	Proxy              *url.URL
	TLS                *tlscommon.TLSConfig
	Username, Password string
	Parameters         map[string]string
	Index              outil.Selector
	Pipeline           *outil.Selector
	Timeout            time.Duration
	CompressionLevel   int
	Observer           outputs.Observer
	BatchPublish       bool
	Headers            map[string]string
	ContentType        string
	Format             string
	Logger             *logp.Logger
}

// Connection struct
type Connection struct {
	URL         string
	Username    string
	Password    string
	http        *http.Client
	connected   bool
	encoder     bodyEncoder
	ContentType string
	Logger      *logp.Logger
}

type eventRaw map[string]json.RawMessage

type event struct {
	Timestamp time.Time `json:"@timestamp"`
	Fields    mapstr.M  `json:"-"`
}

// NewClient instantiate a client.
func NewClient(s ClientSettings) (*Client, error) {
	proxy := http.ProxyFromEnvironment
	if s.Proxy != nil {
		proxy = http.ProxyURL(s.Proxy)
	}
	logger.Infof("HTTP URL: %s", s.URL)
	var dialer, tlsDialer transport.Dialer
	var err error

	dialer = transport.NetDialer(s.Timeout)
	tlsDialer = transport.TLSDialer(dialer, s.TLS, s.Timeout)

	if st := s.Observer; st != nil {
		dialer = transport.StatsDialer(dialer, st)
		tlsDialer = transport.StatsDialer(tlsDialer, st)
	}
	params := s.Parameters
	var encoder bodyEncoder
	compression := s.CompressionLevel
	if compression == 0 {
		switch s.Format {
		case "json":
			encoder = newJSONEncoder(nil)
		case "json_lines":
			encoder = newJSONLinesEncoder(nil)
		}
	} else {
		switch s.Format {
		case "json":
			encoder, err = newGzipEncoder(compression, nil)
		case "json_lines":
			encoder, err = newGzipLinesEncoder(compression, nil)
		}
		if err != nil {
			return nil, err
		}
	}
	client := &Client{
		Connection: Connection{
			URL:         s.URL,
			Username:    s.Username,
			Password:    s.Password,
			ContentType: s.ContentType,
			http: &http.Client{
				Transport: &http.Transport{
					Dial:    dialer.Dial,
					DialTLS: tlsDialer.Dial,
					Proxy:   proxy,
				},
				Timeout: s.Timeout,
			},
			encoder: encoder,
			Logger:  s.Logger,
		},
		params:           params,
		compressionLevel: compression,
		proxyURL:         s.Proxy,
		batchPublish:     s.BatchPublish,
		headers:          s.Headers,
		format:           s.Format,
	}

	return client, nil
}

// Clone clones a client.
func (client *Client) Clone() *Client {
	// when cloning the connection callback and params are not copied. A
	// client's close is for example generated for topology-map support. With params
	// most likely containing the ingest node pipeline and default callback trying to
	// create install a template, we don't want these to be included in the clone.
	c, _ := NewClient(
		ClientSettings{
			URL:              client.URL,
			Proxy:            client.proxyURL,
			TLS:              client.tlsConfig,
			Username:         client.Username,
			Password:         client.Password,
			Parameters:       client.params,
			Timeout:          client.http.Timeout,
			CompressionLevel: client.compressionLevel,
			BatchPublish:     client.batchPublish,
			Headers:          client.headers,
			ContentType:      client.ContentType,
			Format:           client.format,
		},
	)
	return c
}

// Connect establishes a connection to the clients sink.
func (conn *Connection) Connect(ctx context.Context) error {
	conn.connected = true
	return nil
}

// Close closes a connection.
func (conn *Connection) Close() error {
	conn.connected = false
	return nil
}

func (client *Client) String() string {
	return client.URL
}

// Publish sends events to the clients sink.
func (client *Client) Publish(_ context.Context, batch publisher.Batch) error {
	events := batch.Events()
	rest, err := client.publishEvents(events)
	if len(rest) == 0 {
		batch.ACK()
	} else {
		batch.RetryEvents(rest)
	}
	return err
}

// PublishEvents posts all events to the http endpoint. On error a slice with all
// events not published will be returned.
func (client *Client) publishEvents(data []publisher.Event) ([]publisher.Event, error) {
	begin := time.Now()
	if len(data) == 0 {
		return nil, nil
	}
	if !client.connected {
		return data, ErrNotConnected
	}
	var failedEvents []publisher.Event
	sendErr := error(nil)
	if client.batchPublish {
		// Publish events in bulk
		logger.Infof("Publishing events in batch.")
		sendErr = client.BatchPublishEvent(data)
		if sendErr != nil {
			return data, sendErr
		}
	} else {
		logger.Infof("Publishing events one by one.")
		// 进这里单条发送
		for index, msg := range data {
			sendErr = client.PublishEvent(msg)
			if sendErr != nil {
				// return the rest of the data with the error
				failedEvents = data[index:]
				break
			}
		}
	}
	logger.Infof("PublishEvents: %d metrics have been published over HTTP in %v.", len(data), time.Now().Sub(begin))
	if len(failedEvents) > 0 {
		return failedEvents, sendErr
	}
	return nil, nil
}

// BatchPublishEvent publish a single event to output.
func (client *Client) BatchPublishEvent(data []publisher.Event) error {
	if !client.connected {
		return ErrNotConnected
	}
	var events = make([]eventRaw, len(data))
	for i, event := range data {
		events[i] = makeEvent(&event.Content)
	}
	status, _, err := client.request("POST", client.params, events, client.headers)
	if err != nil {
		logger.Warn("Fail to insert a single event: %s", err)
		if err == ErrJSONEncodeFailed {
			// don't retry unencodable values
			return nil
		}
	}
	switch {
	case status == 500 || status == 400: //server error or bad input, don't retry
		return nil
	case status >= 300:
		// retry
		return err
	}
	return nil
}

// PublishEvent publish a single event to output.
func (client *Client) PublishEvent(data publisher.Event) error {
	if !client.connected {
		return ErrNotConnected
	}
	event := data
	//status, _, err := client.request("POST", client.params, makeEvent(&event.Content), client.headers)
	status, _, err := client.request("POST", client.params, makeEventFromDissect(&event.Content), client.headers)
	if err != nil {
		logger.Warn("Fail to insert a single event: %s", err)
		if err == ErrJSONEncodeFailed {
			// don't retry unencodable values
			return nil
		}
	}
	switch {
	case status == 500 || status == 400: //server error or bad input, don't retry
		return nil
	case status >= 300:
		// retry
		return err
	}
	if !client.connected {
		return ErrNotConnected
	}
	return nil
}

func (conn *Connection) request(method string, params map[string]string, body interface{}, headers map[string]string) (int, []byte, error) {
	urlStr := addToURL(conn.URL, params)

	if body == nil {
		return conn.execRequest(method, urlStr, nil, headers)
	}

	if err := conn.encoder.Marshal(body); err != nil {
		logger.Warn("Failed to json encode body (%v): %#v", err, body)
		return 0, nil, ErrJSONEncodeFailed
	}
	return conn.execRequest(method, urlStr, conn.encoder.Reader(), headers)
}

func (conn *Connection) execRequest(method, url string, body io.Reader, headers map[string]string) (int, []byte, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		logger.Warn("Failed to create request: %v", err)
		return 0, nil, err
	}
	if body != nil {
		conn.encoder.AddHeader(&req.Header, conn.ContentType)
	}

	//bs, err := httputil.DumpRequest(req, true)
	//if err != nil {
	//	logger.Warn("Failed to dump request: %v", err)
	//}
	//fmt.Println(string(bs))
	return conn.execHTTPRequest(req, headers)
}

func (conn *Connection) execHTTPRequest(req *http.Request, headers map[string]string) (int, []byte, error) {
	req.Header.Add("Accept", "application/json")
	for key, value := range headers {
		req.Header.Add(key, value)
	}
	if conn.Username != "" || conn.Password != "" {
		req.SetBasicAuth(conn.Username, conn.Password)
	}
	resp, err := conn.http.Do(req)
	if err != nil {
		conn.connected = false
		return 0, nil, err
	}
	defer closing(resp.Body)

	status := resp.StatusCode
	if status >= 300 {
		conn.connected = false
		return status, nil, fmt.Errorf("%v", resp.Status)
	}
	obj, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		conn.connected = false
		return status, nil, err
	}
	return status, obj, nil
}

func closing(c io.Closer) {
	err := c.Close()
	if err != nil {
		logger.Warn("Close failed with: %v", err)
	}
}

// this should ideally be in enc.go
func makeEvent(v *beat.Event) map[string]json.RawMessage {
	// Inline not supported,
	// HT: https://stackoverflow.com/questions/49901287/embed-mapstringstring-in-go-json-marshaling-without-extra-json-property-inlin
	type event0 event // prevent recursion
	e := event{Timestamp: v.Timestamp.UTC(), Fields: mapstr.M(v.Fields)}
	b, err := json.Marshal(event0(e))
	if err != nil {
		logger.Warn("Error encoding event to JSON: %v", err)
	}

	var eventMap map[string]json.RawMessage
	err = json.Unmarshal(b, &eventMap)
	if err != nil {
		logger.Warn("Error decoding JSON to map: %v", err)
	}
	// Add the individual fields to the map, flatten "Fields"
	for j, k := range e.Fields {
		b, err = json.Marshal(k)
		if err != nil {
			logger.Warn("Error encoding map to JSON: %v", err)
		}
		eventMap[j] = b
	}
	return eventMap
}

// makeEventFromDissect 单条发送大数据平台固定格式
func makeEventFromDissect(v *beat.Event) map[string]json.RawMessage {
	// request_time 使用 timestamp processer 处理过的日志时间
	var event0 = mapstr.M{
		"request_time": v.Timestamp.UTC(),
	}

	if dissect, ok := v.Fields["dissect"]; ok {
		err := copier.Copy(&event0, dissect)
		if err != nil {
			logger.Warn("Error decoding dissect to JSON: %v", err)
			return nil
		}
		if token, ok := event0["http_authorization"]; ok {
			delete(event0, "http_authorization")
			if tokenStr, ok := token.(string); ok && tokenStr != "-" {
				parseToken, err := ParseToken(tokenStr)
				if err == nil {
					event0["user_id"] = parseToken.UserID
					event0["user_key"] = parseToken.UserKey
				} else {
					logger.Warnf("Error parsing token: %v", err)
				}
			}
		}
	}

	if request_body, ok := event0["request_body"]; ok {
		// nginx 记录 body 的格式是  "{\\x22createTime\\x22:null}", 需要对 \\22 处理，转换为正常的 json
		event0["request_body"], _ = strconv.Unquote(fmt.Sprintf(`"%s"`, request_body))
	}

	if _, ok := event0["request_uri"]; !ok {
		return nil
	}

	request_url, _ := url.ParseRequestURI(fmt.Sprint(event0["request_uri"]))
	api_url := url.URL{
		Scheme: fmt.Sprint(event0["scheme"]),
		Host:   fmt.Sprint(event0["host"]),
		Path:   request_url.Path,
	}
	event0["api_url"] = api_url.String()
	event0["query_string"] = request_url.RawQuery

	// nginx_type 字段, 1:资管  2:云管  3:信令
	if _, ok := event0["nginx_type"]; !ok {
		if strings.HasPrefix(request_url.Path, "/signalman/signal") {
			event0["nginx_type"] = 3
		} else {
			event0["nginx_type"] = 1
		}
	}

	b, err := json.Marshal(event0)
	if err != nil {
		logger.Warn("Error encoding event to JSON: %v", err)
	}

	var eventMap map[string]json.RawMessage
	err = json.Unmarshal(b, &eventMap)
	if err != nil {
		logger.Warn("Error decoding JSON to map: %v", err)
	}
	return eventMap
}

type Claims struct {
	UserID       int64  `json:"user_id"`
	UserKey      string `json:"user_key"`
	IsAllowLogin bool   `json:"isAllowLogin"`
	IsAppUser    bool   `json:"isAppUser"`
	Username     string `json:"username"`
	jwt.StandardClaims
}

func ParseToken(token string) (*Claims, error) {
	if strings.HasPrefix(token, "Bearer ") {
		token = strings.TrimSpace(strings.TrimLeft(token, "Bearer"))
	}

	// 有 jwtSecret 解析
	//tokenClaims, err := jwt.ParseWithClaims(token, &Claims{}, func(token *jwt.Token) (interface{}, error) {
	//	return "jwtSecret", nil
	//})
	//if err != nil {
	//	return nil, err
	//}
	//
	//if tokenClaims != nil {
	//	if claims, ok := tokenClaims.Claims.(*Claims); ok && tokenClaims.Valid {
	//		return claims, nil
	//	}
	//}
	//
	//return nil, err

	// 没有 jwtSecret，只解析明文部分
	jtoken, parts, err := new(jwt.Parser).ParseUnverified(token, &Claims{})
	if err != nil {
		return nil, err
	}
	_ = parts

	if claims, ok := jtoken.Claims.(*Claims); !ok {
		return nil, err
	} else {
		return claims, nil
	}
}
