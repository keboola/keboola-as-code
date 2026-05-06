package definition

// SinkTypeKaiAgent is the sink type identifier for the kai-agent sink.
const SinkTypeKaiAgent = SinkType("kaiAgent")

// KaiAgentMode selects which kai-agent endpoint is called per record.
type KaiAgentMode string

const (
	// KaiAgentModeChat sends each record as a user message to POST /api/chat.
	// The response is an SSE stream; we fire-and-forget after HTTP 200.
	KaiAgentModeChat KaiAgentMode = "chat"
	// KaiAgentModeSuggestions sends each record to POST /api/suggestions and reads the JSON response.
	KaiAgentModeSuggestions KaiAgentMode = "suggestions"
)

// KaiAgentSuggestionsContext matches the enum expected by POST /api/suggestions.
type KaiAgentSuggestionsContext string

const (
	KaiAgentSuggestionsContextDashboard    KaiAgentSuggestionsContext = "dashboard"
	KaiAgentSuggestionsContextJobDetail    KaiAgentSuggestionsContext = "job-detail"
	KaiAgentSuggestionsContextConfigDetail KaiAgentSuggestionsContext = "configuration-detail"
)

// KaiAgentSink configures a sink that forwards each incoming record to kai-agent.keboola.com.
// The target endpoint is selected by Mode.
type KaiAgentSink struct {
	// Mode selects the endpoint: "chat" or "suggestions".
	Mode KaiAgentMode `json:"mode" validate:"required,oneof=chat suggestions"`

	// --- Chat mode fields (Mode = "chat") ---

	// ChatID is an optional fixed UUID used as the chat identifier.
	// When empty a new UUID is generated for every record (creating a new chat per event).
	// When set, all records are appended to the same chat session.
	ChatID string `json:"chatId,omitempty"`

	// MessageTemplate is an optional Jsonnet template evaluated against the incoming request.
	// Its output must be a plain string; that string becomes the text of the chat message.
	// Available functions: Body(), Header(), Ip(), Now() — same as in table column templates.
	// When empty the raw JSON body of the incoming request is used as the message text.
	MessageTemplate string `json:"messageTemplate,omitempty"`

	// BranchID is an optional Keboola branch ID forwarded to the chat for context.
	// 0 means no branch is specified.
	BranchID int `json:"branchId,omitempty"`

	// --- Suggestions mode fields (Mode = "suggestions") ---

	// SuggestionsContext is the UI context passed to POST /api/suggestions.
	// Required when Mode = "suggestions".
	SuggestionsContext KaiAgentSuggestionsContext `json:"suggestionsContext,omitempty" validate:"required_if=Mode suggestions"`

	// DataTemplate is an optional Jsonnet template evaluated against the incoming request.
	// Its output must be a JSON object; that object is forwarded as the "data" field.
	// When empty the full parsed JSON body is forwarded as-is.
	DataTemplate string `json:"dataTemplate,omitempty"`
}
