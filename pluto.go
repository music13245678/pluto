package pluto

import (
	"context"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	Downloaded uint64
	Speed      uint64
	Size       uint64
}

type fileMetaData struct {
	url                *url.URL
	Size               uint64
	Name               string
	MultipartSupported bool
}

type Pluto struct {
	StatsChan   chan *Stats
	Finished    chan struct{}
	connections uint
	verbose     bool
	headers     []string
	downloaded  uint64
	MetaData    fileMetaData
	startTime   time.Time
	workers     []*worker
	client      *http.Client
}

type Result struct {
	FileName  string
	Size      uint64
	AvgSpeed  float64
	TimeTaken time.Duration
}

type Proxy struct {
	Host string `json:"host"`
	Port int    `json:"port"`
	User string `json:"user"`
	Pass string `json:"pass"`
}

func New(up *url.URL, headers []string, connections uint, verbose bool, proxy *Proxy) (*Pluto, error) {
	p := &Pluto{
		connections: connections,
		headers:     headers,
		verbose:     verbose,
		StatsChan:   make(chan *Stats, 10), // Thêm buffer để tránh block
		Finished:    make(chan struct{}),
		client:      &http.Client{},
	}

	if proxy != nil {
		p.client = &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyURL(&url.URL{
					Host: fmt.Sprintf("%s:%d", proxy.Host, proxy.Port),
					User: url.UserPassword(proxy.User, proxy.Pass),
				}),
				DialContext: (&net.Dialer{
					Timeout:   10 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:        100,
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 10 * time.Second,
			},
		}
	}

	err := p.fetchMeta(up, headers)
	if err != nil {
		return nil, err
	}

	if !p.MetaData.MultipartSupported {
		p.connections = 1
	}

	return p, nil
}

func (p *Pluto) Download(ctx context.Context, w io.WriterAt) (*Result, error) {
	p.startTime = time.Now()
	perPartLimit := p.MetaData.Size / uint64(p.connections)
	difference := p.MetaData.Size % uint64(p.connections)

	p.workers = make([]*worker, p.connections)
	for i := uint(0); i < p.connections; i++ {
		begin := perPartLimit * uint64(i)
		end := perPartLimit * (uint64(i)+1)
		if i == p.connections-1 {
			end += difference
		}

		p.workers[i] = &worker{
			begin:   begin,
			end:     end,
			url:     p.MetaData.url,
			writer:  w,
			headers: p.headers,
			verbose: p.verbose,
			ctx:     ctx,
			client:  p.client,
		}
	}

	// Truyền ctx vào startDownload để kiểm soát toàn bộ vòng đời tải
	err := p.startDownload(ctx)
	if err != nil {
		return nil, err
	}

	tt := time.Since(p.startTime)
	r := &Result{
		TimeTaken: tt,
		FileName:  p.MetaData.Name,
		Size:      p.MetaData.Size,
		AvgSpeed:  float64(p.MetaData.Size) / float64(tt.Seconds()),
	}

	return r, nil
}

func (p *Pluto) startDownload(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(len(p.workers))

	cerr := make(chan error, len(p.workers))
	var downloaded uint64

	// Stats System cải tiến: Thoát ngay khi ctx Done
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-p.Finished:
				return
			case <-ticker.C:
				dled := atomic.LoadUint64(&downloaded)
				speed := dled - p.downloaded
				
				// Gửi stats không chặn (non-blocking)
				select {
				case p.StatsChan <- &Stats{
					Downloaded: dled,
					Speed:      speed * 2,
					Size:       p.MetaData.Size,
				}:
				default:
				}
				p.downloaded = dled
			}
		}
	}()

	for _, w := range p.workers {
		go func(wrk *worker) {
			defer wg.Done()
			
			// 1. Download part
			downloadPart, err := wrk.download()
			if err != nil {
				if ctx.Err() == nil { // Chỉ báo lỗi nếu không phải chủ động cancel
					cerr <- err
				}
				return
			}
			defer downloadPart.Close()

			// 2. Copy data - Phải check context bên trong copyAt (nếu có thể)
			_, err = wrk.copyAt(downloadPart, &downloaded)
			if err != nil {
				if ctx.Err() == nil {
					cerr <- err
				}
				return
			}
		}(w)
	}

	// Goroutine để đóng Finished channel khi tất cả worker xong
	go func() {
		wg.Wait()
		close(p.Finished)
	}()

	// Đợi sự kiện đầu tiên xảy ra: Lỗi, Xong, hoặc Bị Cancel
	select {
	case err := <-cerr:
		if err != nil {
			return err
		}
		return nil
	case <-p.Finished:
		return nil
	case <-ctx.Done():
		return ctx.Err() // Trả về lỗi context để Downloader biết là do chủ động ngắt
	}
}

// Giữ nguyên fetchMeta nhưng tối ưu đóng Body
func (p *Pluto) fetchMeta(u *url.URL, headers []string) error {
	req, err := http.NewRequest("HEAD", u.String(), nil)
	if err != nil {
		return err
	}

	for _, v := range headers {
		vsp := strings.Index(v, ":")
		if vsp > 0 {
			req.Header.Set(v[:vsp], v[vsp+1:])
		}
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("status code: %d", resp.StatusCode)
	}

	p.MetaData = fileMetaData{
		Size:               uint64(resp.ContentLength),
		Name:               filepath.Base(u.Path),
		url:                u,
		MultipartSupported: resp.Header.Get("Accept-Ranges") == "bytes",
	}
	return nil
}
