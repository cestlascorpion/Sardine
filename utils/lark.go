package utils

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

type Content struct {
	Text string `json:"text"`
}

type TextMessage struct {
	Timestamp string   `json:"timestamp,omitempty"`
	Sign      string   `json:"sign,omitempty"`
	MsgType   string   `json:"msg_type,omitempty"`
	Content   *Content `json:"content,omitempty"`
}

type Response struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func Sign(secret string, timestamp int64) (string, error) {
	stringToSign := fmt.Sprintf("%v", timestamp) + "\n" + secret

	var data []byte
	h := hmac.New(sha256.New, []byte(stringToSign))
	_, err := h.Write(data)
	if err != nil {
		return "", err
	}

	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))
	return signature, nil
}

type LarkBot struct {
	*RestyClient
	webhook string
	secret  string
}

func NewLarkBot(ctx context.Context, config *Config) (*LarkBot, error) {
	client, err := NewRestyClient(&http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 3,
			MaxConnsPerHost:     5,
			IdleConnTimeout:     time.Second * 90,
		},
		Timeout: time.Second * 5,
	})
	if err != nil {
		log.Errorf("new resty client err %+v", err)
		return nil, err
	}

	return &LarkBot{
		RestyClient: client,
		webhook:     config.Reporter.Webhook,
		secret:      config.Reporter.Secret,
	}, nil
}

func (l *LarkBot) SendMsg(ctx context.Context, format string, args ...interface{}) {
	if len(l.webhook) == 0 || len(l.secret) == 0 {
		return
	}
	go l.sendMsg(ctx, format, args...)
}

func (l *LarkBot) sendMsg(ctx context.Context, format string, args ...interface{}) {
	timestamp := time.Now().Unix()
	sign, err := Sign(l.secret, timestamp)
	if err != nil {
		log.Errorf("sign err %+v", err)
		return
	}

	req := TextMessage{
		Timestamp: strconv.FormatInt(timestamp, 10),
		Sign:      sign,
		MsgType:   "text",
		Content: &Content{
			Text: fmt.Sprintf(format, args...),
		},
	}

	result, err := l.POST(ctx, l.webhook, "", req, time.Second*2)
	if err != nil {
		log.Errorf("post err %+v", err)
		return
	}

	var resp Response
	err = json.Unmarshal(result, &resp)
	if err != nil {
		log.Warnf("json.unmarshal err %+v", err)
		return
	}
	if resp.Code != 0 {
		log.Warnf("resp.Code %d, resp.Msg %s", resp.Code, resp.Msg)
		return
	}

	log.Debugf("req %+v resp %+v", req, resp)
}
