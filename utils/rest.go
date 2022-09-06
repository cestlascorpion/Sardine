package utils

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

type RestyClient struct {
	hc *http.Client
}

func NewRestyClient(hc *http.Client) (*RestyClient, error) {
	if hc == nil || hc.Transport == nil {
		return nil, errors.New("invalid parameter")
	}
	return &RestyClient{
		hc: hc,
	}, nil
}

func (r *RestyClient) GET(ctx context.Context, url, token string, body interface{}, timeout time.Duration) ([]byte, error) {
	client := resty.NewWithClient(r.hc).SetTimeout(timeout).EnableTrace().
		SetHeader("Content-Type", "application/json;charset=utf-8").
		SetHeader("Accept", "application/json")
	if len(token) > 0 {
		client.SetHeader("token", token)
	}
	log.Debugf("get url %s", url)
	resp, err := client.R().SetBody(body).SetContext(ctx).Get(url)
	if err != nil {
		log.Errorf("resty get err %+v", err)
		return nil, err
	}
	log.Debugf("%+v", resp.Request.TraceInfo())
	return resp.Body(), nil
}

func (r *RestyClient) POST(ctx context.Context, url, token string, body interface{}, timeout time.Duration) ([]byte, error) {
	client := resty.NewWithClient(r.hc).SetTimeout(timeout).EnableTrace().
		SetHeader("Content-Type", "application/json;charset=utf-8").
		SetHeader("Accept", "application/json")
	if len(token) > 0 {
		client.SetHeader("token", token)
	}
	log.Debugf("post url %s", url)
	resp, err := client.R().SetBody(body).SetContext(ctx).Post(url)
	if err != nil {
		log.Errorf("resty post err %+v", err)
		return nil, err
	}
	log.Debugf("%+v", resp.Request.TraceInfo())
	return resp.Body(), nil
}

func (r *RestyClient) PUT(ctx context.Context, url, token string, body interface{}, timeout time.Duration) ([]byte, error) {
	client := resty.NewWithClient(r.hc).SetTimeout(timeout).EnableTrace().
		SetHeader("Content-Type", "application/json;charset=utf-8").
		SetHeader("Accept", "application/json")
	if len(token) > 0 {
		client.SetHeader("token", token)
	}
	log.Debugf("put url %s", url)
	resp, err := client.R().SetBody(body).SetContext(ctx).Put(url)
	if err != nil {
		log.Errorf("resty put err %+v", err)
		return nil, err
	}
	log.Debugf("%+v", resp.Request.TraceInfo())
	return resp.Body(), nil
}

func (r *RestyClient) DELETE(ctx context.Context, url, token string, body interface{}, timeout time.Duration) ([]byte, error) {
	client := resty.NewWithClient(r.hc).SetTimeout(timeout).EnableTrace().
		SetHeader("Content-Type", "application/json;charset=utf-8").
		SetHeader("Accept", "application/json")
	if len(token) > 0 {
		client.SetHeader("token", token)
	}
	log.Debugf("del url %s", url)
	resp, err := client.R().SetBody(body).SetContext(ctx).Delete(url)
	if err != nil {
		log.Errorf("resty Delete err %+v", err)
		return nil, err
	}
	log.Debugf("%+v", resp.Request.TraceInfo())
	return resp.Body(), nil
}

func GET(ctx context.Context, url, token string, body interface{}, timeout time.Duration) ([]byte, error) {
	client := resty.New().SetTimeout(timeout).EnableTrace().
		SetHeader("Content-Type", "application/json;charset=utf-8").
		SetHeader("Accept", "application/json").SetCloseConnection(true)
	if len(token) > 0 {
		client.SetHeader("token", token)
	}
	log.Debugf("get url %s", url)
	resp, err := client.R().SetBody(body).SetContext(ctx).Get(url)
	if err != nil {
		log.Errorf("resty get err %+v", err)
		return nil, err
	}
	log.Debugf("%+v", resp.Request.TraceInfo())
	return resp.Body(), nil
}

func POST(ctx context.Context, url, token string, body interface{}, timeout time.Duration) ([]byte, error) {
	client := resty.New().SetTimeout(timeout).EnableTrace().
		SetHeader("Content-Type", "application/json;charset=utf-8").
		SetHeader("Accept", "application/json").SetCloseConnection(true)
	if len(token) > 0 {
		client.SetHeader("token", token)
	}
	log.Debugf("post url %s", url)
	resp, err := client.R().SetBody(body).SetContext(ctx).Post(url)
	if err != nil {
		log.Errorf("resty post err %+v", err)
		return nil, err
	}
	log.Debugf("%+v", resp.Request.TraceInfo())
	return resp.Body(), nil
}

func PUT(ctx context.Context, url, token string, body interface{}, timeout time.Duration) ([]byte, error) {
	client := resty.New().SetTimeout(timeout).EnableTrace().
		SetHeader("Content-Type", "application/json;charset=utf-8").
		SetHeader("Accept", "application/json").SetCloseConnection(true)
	if len(token) > 0 {
		client.SetHeader("token", token)
	}
	log.Debugf("put url %s", url)
	resp, err := client.R().SetBody(body).SetContext(ctx).Put(url)
	if err != nil {
		log.Errorf("resty put err %+v", err)
		return nil, err
	}
	log.Debugf("%+v", resp.Request.TraceInfo())
	return resp.Body(), nil
}

func DELETE(ctx context.Context, url, token string, body interface{}, timeout time.Duration) ([]byte, error) {
	client := resty.New().SetTimeout(timeout).EnableTrace().
		SetHeader("Content-Type", "application/json;charset=utf-8").
		SetHeader("Accept", "application/json").SetCloseConnection(true)
	if len(token) > 0 {
		client.SetHeader("token", token)
	}
	log.Debugf("del url %s", url)
	resp, err := client.R().SetBody(body).SetContext(ctx).Delete(url)
	if err != nil {
		log.Errorf("resty Delete err %+v", err)
		return nil, err
	}
	log.Debugf("%+v", resp.Request.TraceInfo())
	return resp.Body(), nil
}
