package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"kiro-go/config"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	webSearchToolName        = "web_search"
	kiroWebSearchToolName    = "webSearch"
	kiroWebFetchToolName     = "fetch"
	maxHostedWebSearchRounds = 3
	maxHostedFetchBytes      = 2 << 20
	maxHostedFetchTextChars  = 20000
)

type WebSearchResult struct {
	Title         string `json:"title"`
	URL           string `json:"url"`
	Snippet       string `json:"snippet"`
	PublishedDate string `json:"publishedDate,omitempty"`
}

func (r *WebSearchResult) UnmarshalJSON(data []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	r.Title = scalarString(raw["title"])
	r.URL = scalarString(firstExisting(raw, "url", "link"))
	r.Snippet = scalarString(firstExisting(raw, "snippet", "description", "text"))
	r.PublishedDate = scalarString(firstExisting(raw, "publishedDate", "published_date", "date"))
	return nil
}

func CallKiroAPIWithHostedTools(account *config.Account, payload *KiroPayload, callback *KiroStreamCallback) error {
	if payload == nil || !payload.HostedWebSearch {
		return CallKiroAPI(account, payload, callback)
	}

	current := payload
	var hostedResults []KiroToolResult
	hostedToolNames := make(map[string]string)
	emittedDownstream := false
	for round := 0; round < maxHostedWebSearchRounds; round++ {
		var hostedToolUses []KiroToolUse
		wrapped := callback
		if callback != nil {
			next := *callback
			next.OnText = func(text string, isThinking bool) {
				if strings.TrimSpace(text) != "" && !isThinking {
					emittedDownstream = true
				}
				if callback.OnText != nil {
					callback.OnText(text, isThinking)
				}
			}
			next.OnToolUse = func(tu KiroToolUse) {
				if isHostedWebToolUse(tu) {
					hostedToolUses = append(hostedToolUses, tu)
					return
				}
				emittedDownstream = true
				if callback.OnToolUse != nil {
					callback.OnToolUse(tu)
				}
			}
			wrapped = &next
		}

		if err := CallKiroAPI(account, current, wrapped); err != nil {
			return err
		}
		if len(hostedToolUses) == 0 {
			if !emittedDownstream && len(hostedResults) > 0 {
				emitHostedToolResultsFallback(callback, hostedResults, hostedToolNames)
			}
			return nil
		}

		results, err := resolveHostedWebToolResults(account, hostedToolUses)
		if err != nil {
			return err
		}
		for _, tu := range hostedToolUses {
			if tu.ToolUseID != "" && tu.Name != "" {
				hostedToolNames[tu.ToolUseID] = tu.Name
			}
		}
		hostedResults = append(hostedResults, results...)
		current = buildWebSearchFollowupPayload(current, hostedToolUses, results)
		if current == nil {
			if !emittedDownstream && len(hostedResults) > 0 {
				emitHostedToolResultsFallback(callback, hostedResults, hostedToolNames)
			}
			return nil
		}
	}
	if !emittedDownstream && len(hostedResults) > 0 {
		emitHostedToolResultsFallback(callback, hostedResults, hostedToolNames)
		return nil
	}
	if len(hostedResults) > 0 {
		emitHostedToolResultsFallback(callback, hostedResults, hostedToolNames)
		return nil
	}
	if callback != nil && callback.OnText != nil {
		callback.OnText(fmt.Sprintf("Web search stopped after %d internal rounds without a final answer.", maxHostedWebSearchRounds), false)
		return nil
	}
	return nil
}

func emitHostedToolResultsFallback(callback *KiroStreamCallback, results []KiroToolResult, names map[string]string) {
	if callback == nil || callback.OnText == nil {
		return
	}
	text := hostedToolResultsFallbackText(results, names)
	if strings.TrimSpace(text) != "" {
		callback.OnText(text, false)
	}
}

func hostedToolResultsFallbackText(results []KiroToolResult, names map[string]string) string {
	text := narrateToolResults(results, names)
	if strings.TrimSpace(text) == "" {
		text = buildToolResultsContinuation(results)
	}
	return truncateRunes(text, maxHostedFetchTextChars)
}

func isHostedWebSearchToolType(toolType string) bool {
	t := strings.ToLower(strings.TrimSpace(toolType))
	return t == "web_search" || t == "web_search_preview" ||
		strings.HasPrefix(t, "web_search_") ||
		strings.HasPrefix(t, "web_search_preview_")
}

func isHostedWebSearchToolName(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	n = strings.ReplaceAll(n, "_", "")
	n = strings.ReplaceAll(n, "-", "")
	n = strings.ReplaceAll(n, " ", "")
	return n == "websearch"
}

func isHostedWebFetchToolName(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	n = strings.ReplaceAll(n, "_", "")
	n = strings.ReplaceAll(n, "-", "")
	n = strings.ReplaceAll(n, " ", "")
	return n == "fetch" || n == "webfetch"
}

func webSearchInputSchema() interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"query": map[string]interface{}{
				"type":        "string",
				"description": "Search query",
			},
		},
		"required": []string{"query"},
	}
}

func makeWebSearchKiroTool() KiroToolWrapper {
	wrapper := KiroToolWrapper{}
	wrapper.ToolSpecification.Name = kiroWebSearchToolName
	wrapper.ToolSpecification.Description = "Search the web for current information."
	wrapper.ToolSpecification.InputSchema = InputSchema{JSON: webSearchInputSchema()}
	return wrapper
}

func webFetchInputSchema() interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"url": map[string]interface{}{
				"type":        "string",
				"description": "Public HTTP or HTTPS URL to fetch",
			},
			"prompt": map[string]interface{}{
				"type":        "string",
				"description": "Optional extraction instructions",
			},
		},
		"required": []string{"url"},
	}
}

func makeWebFetchKiroTool() KiroToolWrapper {
	wrapper := KiroToolWrapper{}
	wrapper.ToolSpecification.Name = kiroWebFetchToolName
	wrapper.ToolSpecification.Description = "Fetch a public web page by URL and return readable text."
	wrapper.ToolSpecification.InputSchema = InputSchema{JSON: webFetchInputSchema()}
	return wrapper
}

func isHostedWebToolUse(tu KiroToolUse) bool {
	return isWebSearchToolUse(tu) || isWebFetchToolUse(tu)
}

func isWebSearchToolUse(tu KiroToolUse) bool {
	name := strings.ToLower(strings.TrimSpace(tu.Name))
	name = strings.ReplaceAll(name, "_", "")
	name = strings.ReplaceAll(name, "-", "")
	return name == "websearch"
}

func isWebFetchToolUse(tu KiroToolUse) bool {
	name := strings.ToLower(strings.TrimSpace(tu.Name))
	name = strings.ReplaceAll(name, "_", "")
	name = strings.ReplaceAll(name, "-", "")
	return name == "fetch" || name == "webfetch"
}

func resolveHostedWebToolResults(account *config.Account, toolUses []KiroToolUse) ([]KiroToolResult, error) {
	results := make([]KiroToolResult, 0, len(toolUses))
	for _, tu := range toolUses {
		if !isHostedWebToolUse(tu) {
			return nil, fmt.Errorf("unsupported hosted tool: %s", tu.Name)
		}

		text := ""
		status := "success"
		var err error
		if isWebSearchToolUse(tu) {
			var searchResults []WebSearchResult
			searchResults, err = performKiroWebSearch(context.Background(), account, tu)
			text = formatWebSearchResults(searchResults)
		} else {
			text, err = performHostedWebFetch(context.Background(), account, tu)
		}
		if err != nil {
			status = "error"
			text = tu.Name + " failed: " + err.Error()
		}

		results = append(results, KiroToolResult{
			ToolUseID: tu.ToolUseID,
			Content:   []KiroResultContent{{Text: text}},
			Status:    status,
		})
	}
	return results, nil
}

func buildWebSearchFollowupPayload(base *KiroPayload, toolUses []KiroToolUse, results []KiroToolResult) *KiroPayload {
	if base == nil {
		return nil
	}
	next := *base
	current := base.ConversationState.CurrentMessage.UserInputMessage
	historyUser := current
	historyUser.UserInputMessageContext = nil

	history := append([]KiroHistoryMessage(nil), base.ConversationState.History...)
	history = append(history,
		KiroHistoryMessage{UserInputMessage: &historyUser},
		KiroHistoryMessage{AssistantResponseMessage: &KiroAssistantResponseMessage{ToolUses: toolUses}},
	)

	toolResultNames := collectHistoryToolNames(history)
	history = sanitizeKiroHistory(history, nil)

	content := buildToolResultsContinuation(results)
	if flattened := narrateToolResults(results, toolResultNames); flattened != "" {
		content = flattened
	}

	next.ConversationState.History = history
	next.ConversationState.CurrentMessage.UserInputMessage = KiroUserInputMessage{
		Content: content,
		ModelID: current.ModelID,
		Origin:  current.Origin,
	}
	ctx := &UserInputMessageContext{}
	if current.UserInputMessageContext != nil && len(current.UserInputMessageContext.Tools) > 0 {
		ctx.Tools = current.UserInputMessageContext.Tools
	}
	if len(ctx.Tools) > 0 || len(ctx.ToolResults) > 0 {
		next.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext = ctx
	}
	return &next
}

func performKiroWebSearch(ctx context.Context, account *config.Account, toolUse KiroToolUse) ([]WebSearchResult, error) {
	query := webSearchQueryFromInput(toolUse.Input)
	if query == "" {
		return nil, fmt.Errorf("query is required")
	}

	reqCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      "web_search_tooluse_" + uuid.New().String(),
		"method":  "tools/call",
		"params": map[string]interface{}{
			"name": webSearchToolName,
			"arguments": map[string]interface{}{
				"query": query,
			},
		},
	})

	endpoint := webSearchMCPHost(account) + "/mcp"
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	setKiroHeaders(req, account)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-amzn-codewhisperer-optout", "false")
	req.Header.Set("Amz-Sdk-Request", "attempt=1; max=3")
	req.Header.Set("Amz-Sdk-Invocation-Id", uuid.New().String())

	resp, err := GetRestClientForProxy(ResolveAccountProxyURL(account)).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("MCP HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return parseMCPWebSearchResponse(data)
}

func webSearchMCPHost(account *config.Account) string {
	region := "us-east-1"
	if account != nil && strings.TrimSpace(account.Region) != "" {
		region = strings.TrimSpace(account.Region)
	}
	return "https://q." + region + ".amazonaws.com"
}

func webSearchQueryFromInput(input map[string]interface{}) string {
	for _, key := range []string{"query", "q", "search_query", "searchQuery"} {
		if value := strings.TrimSpace(scalarString(input[key])); value != "" {
			return value
		}
	}
	return ""
}

func webFetchURLFromInput(input map[string]interface{}) string {
	for _, key := range []string{"url", "uri", "href"} {
		if value := strings.TrimSpace(scalarString(input[key])); value != "" {
			return value
		}
	}
	return ""
}

func performHostedWebFetch(ctx context.Context, account *config.Account, toolUse KiroToolUse) (string, error) {
	rawURL := webFetchURLFromInput(toolUse.Input)
	if rawURL == "" {
		return "", fmt.Errorf("url is required")
	}
	if _, err := validatePublicHTTPURL(ctx, rawURL); err != nil {
		return "", err
	}

	reqCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 Kiro-Go WebFetch/1.0")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,text/plain,application/pdf;q=0.8,*/*;q=0.5")

	client := &http.Client{
		Timeout:   45 * time.Second,
		Transport: buildKiroTransport(ResolveAccountProxyURL(account)),
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("stopped after 5 redirects")
			}
			_, err := validatePublicHTTPURL(req.Context(), req.URL.String())
			return err
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	limited := io.LimitReader(resp.Body, maxHostedFetchBytes)
	data, err := io.ReadAll(limited)
	if err != nil {
		return "", err
	}
	text := readableTextFromFetchedBody(data, resp.Header.Get("Content-Type"))
	if strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("no readable text returned")
	}
	return truncateRunes(text, maxHostedFetchTextChars), nil
}

func validatePublicHTTPURL(ctx context.Context, rawURL string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("only http and https URLs are supported")
	}
	host := parsed.Hostname()
	if host == "" {
		return nil, fmt.Errorf("URL host is required")
	}
	if strings.EqualFold(host, "localhost") {
		return nil, fmt.Errorf("localhost is not allowed")
	}
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", host, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("resolve %s: no addresses", host)
	}
	for _, ip := range ips {
		if !isPublicIP(ip) {
			return nil, fmt.Errorf("non-public address is not allowed")
		}
	}
	return parsed, nil
}

func isPublicIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	return !(ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsUnspecified() ||
		ip.IsMulticast())
}

func readableTextFromFetchedBody(data []byte, contentType string) string {
	text := string(data)
	lowerType := strings.ToLower(contentType)
	if strings.Contains(lowerType, "html") || strings.Contains(strings.ToLower(text[:minFetchInt(len(text), 512)]), "<html") {
		return htmlToText(text)
	}
	return normalizeWhitespace(text)
}

func htmlToText(raw string) string {
	s := raw
	s = regexp.MustCompile(`(?is)<script[^>]*>.*?</script>`).ReplaceAllString(s, " ")
	s = regexp.MustCompile(`(?is)<style[^>]*>.*?</style>`).ReplaceAllString(s, " ")
	s = regexp.MustCompile(`(?is)<(br|p|div|li|h[1-6]|tr|section|article|header|footer)[^>]*>`).ReplaceAllString(s, "\n")
	s = regexp.MustCompile(`(?is)<[^>]+>`).ReplaceAllString(s, " ")
	return normalizeWhitespace(html.UnescapeString(s))
}

func normalizeWhitespace(s string) string {
	lines := strings.Split(s, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		out = append(out, strings.Join(fields, " "))
	}
	return strings.Join(out, "\n")
}

func truncateRunes(s string, limit int) string {
	if limit <= 0 {
		return s
	}
	runes := []rune(s)
	if len(runes) <= limit {
		return s
	}
	return string(runes[:limit]) + "\n\n[truncated]"
}

func minFetchInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func parseMCPWebSearchResponse(data []byte) ([]WebSearchResult, error) {
	var rpc struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &rpc); err != nil {
		return nil, err
	}
	if rpc.Error != nil {
		return nil, errors.New(rpc.Error.Message)
	}
	if rpc.Result.IsError {
		if len(rpc.Result.Content) > 0 {
			return nil, errors.New(rpc.Result.Content[0].Text)
		}
		return nil, errors.New("web_search tool error")
	}

	for _, content := range rpc.Result.Content {
		if strings.ToLower(content.Type) != "text" || strings.TrimSpace(content.Text) == "" {
			continue
		}
		if results, ok := decodeWebSearchResults([]byte(content.Text)); ok {
			return capWebSearchResults(results), nil
		}
	}
	if results, ok := decodeWebSearchResults(data); ok {
		return capWebSearchResults(results), nil
	}
	return nil, nil
}

func decodeWebSearchResults(data []byte) ([]WebSearchResult, bool) {
	var wrapped struct {
		Results []WebSearchResult `json:"results"`
	}
	if err := json.Unmarshal(data, &wrapped); err == nil && len(wrapped.Results) > 0 {
		return wrapped.Results, true
	}
	var bare []WebSearchResult
	if err := json.Unmarshal(data, &bare); err == nil && len(bare) > 0 {
		return bare, true
	}
	return nil, false
}

func capWebSearchResults(results []WebSearchResult) []WebSearchResult {
	if len(results) > 5 {
		return results[:5]
	}
	return results
}

func formatWebSearchResults(results []WebSearchResult) string {
	if len(results) == 0 {
		return "No web search results found."
	}

	var b strings.Builder
	for i, result := range results {
		if i > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(strconv.Itoa(i + 1))
		b.WriteString(". ")
		b.WriteString(strings.TrimSpace(result.Title))
		if result.URL != "" {
			b.WriteString("\nURL: ")
			b.WriteString(result.URL)
		}
		if result.PublishedDate != "" {
			b.WriteString("\nPublished: ")
			b.WriteString(result.PublishedDate)
		}
		if result.Snippet != "" {
			b.WriteString("\n")
			b.WriteString(result.Snippet)
		}
	}
	return b.String()
}

func scalarString(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		return fmt.Sprint(t)
	}
}

func firstExisting(m map[string]interface{}, keys ...string) interface{} {
	for _, key := range keys {
		if value, ok := m[key]; ok {
			return value
		}
	}
	return nil
}
