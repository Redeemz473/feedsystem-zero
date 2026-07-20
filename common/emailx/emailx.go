package emailx

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"strings"
	"time"
)

type SMTPConf struct {
	Host     string
	Port     int `json:",default=465"`
	Username string
	Password string
	From     string `json:",optional"`
	FromName string `json:",optional"`
}

func (c SMTPConf) SendVerificationCode(ctx context.Context, to, code string, ttl time.Duration) error {
	if err := c.validate(); err != nil {
		return err
	}
	if _, err := mail.ParseAddress(to); err != nil {
		return fmt.Errorf("invalid recipient email: %w", err)
	}

	subject := "91先生 注册验证码"
	body := fmt.Sprintf("您的注册验证码是：%s\n\n验证码 %d 分钟内有效，请勿泄露给他人。", code, int(ttl.Minutes()))
	msg := buildMessage(c.fromAddress(), to, subject, body)

	return c.send(ctx, to, msg)
}

func (c SMTPConf) validate() error {
	if strings.TrimSpace(c.Host) == "" {
		return errors.New("smtp host is required")
	}
	if c.Port == 0 {
		return errors.New("smtp port is required")
	}
	if strings.TrimSpace(c.Username) == "" {
		return errors.New("smtp username is required")
	}
	if strings.TrimSpace(c.Password) == "" {
		return errors.New("smtp password is required")
	}
	if _, err := mail.ParseAddress(c.fromAddress().Address); err != nil {
		return fmt.Errorf("invalid sender email: %w", err)
	}
	return nil
}

func (c SMTPConf) fromAddress() mail.Address {
	from := strings.TrimSpace(c.From)
	if from == "" {
		from = strings.TrimSpace(c.Username)
	}

	return mail.Address{
		Name:    strings.TrimSpace(c.FromName),
		Address: from,
	}
}

func (c SMTPConf) send(ctx context.Context, to string, msg []byte) error {
	addr := fmt.Sprintf("%s:%d", c.Host, c.Port)
	auth := smtp.PlainAuth("", c.Username, c.Password, c.Host)

	client, err := c.newClient(ctx, addr)
	if err != nil {
		return err
	}
	defer client.Close()

	if ok, _ := client.Extension("AUTH"); ok {
		if err := client.Auth(auth); err != nil {
			return err
		}
	}

	from := c.fromAddress().Address
	if err := client.Mail(from); err != nil {
		return err
	}
	if err := client.Rcpt(to); err != nil {
		return err
	}

	w, err := client.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(msg); err != nil {
		_ = w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}

	return client.Quit()
}

func (c SMTPConf) newClient(ctx context.Context, addr string) (*smtp.Client, error) {
	if c.Port == 465 {
		dialer := tls.Dialer{
			NetDialer: &net.Dialer{Timeout: 10 * time.Second},
			Config:    &tls.Config{ServerName: c.Host, MinVersion: tls.VersionTLS12},
		}
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		return smtp.NewClient(conn, c.Host)
	}

	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	client, err := smtp.NewClient(conn, c.Host)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	if ok, _ := client.Extension("STARTTLS"); ok {
		if err := client.StartTLS(&tls.Config{ServerName: c.Host, MinVersion: tls.VersionTLS12}); err != nil {
			_ = client.Close()
			return nil, err
		}
	}

	return client, nil
}

func buildMessage(from mail.Address, to, subject, body string) []byte {
	headers := map[string]string{
		"From":                      from.String(),
		"To":                        to,
		"Subject":                   mime.QEncoding.Encode("UTF-8", subject),
		"MIME-Version":              "1.0",
		"Content-Type":              `text/plain; charset="UTF-8"`,
		"Content-Transfer-Encoding": "8bit",
	}

	var builder strings.Builder
	for k, v := range headers {
		builder.WriteString(k)
		builder.WriteString(": ")
		builder.WriteString(v)
		builder.WriteString("\r\n")
	}
	builder.WriteString("\r\n")
	builder.WriteString(body)
	builder.WriteString("\r\n")

	return []byte(builder.String())
}
