package dpfm_api_output_formatter

type SDC struct {
	ConnectionKey       string      `json:"connection_key"`
	Result              bool        `json:"result"`
	RedisKey            string      `json:"redis_key"`
	Filepath            string      `json:"filepath"`
	APIStatusCode       int         `json:"api_status_code"`
	RuntimeSessionID    string      `json:"runtime_session_id"`
	BusinessPartnerID   *int        `json:"business_partner"`
	ServiceLabel        string      `json:"service_label"`
	APIType             string      `json:"api_type"`
	Message             interface{} `json:"message"`
	APISchema           string      `json:"api_schema"`
	Accepter            []string    `json:"accepter"`
	Deleted             bool        `json:"deleted"`
	SQLUpdateResult     *bool       `json:"sql_update_result"`
	SQLUpdateError      string      `json:"sql_update_error"`
	SubfuncResult       *bool       `json:"subfunc_result"`
	SubfuncError        string      `json:"subfunc_error"`
	ExconfResult        *bool       `json:"exconf_result"`
	ExconfError         string      `json:"exconf_error"`
	APIProcessingResult *bool       `json:"api_processing_result"`
	APIProcessingError  string      `json:"api_processing_error"`
}

type Message struct {
	Header       *Header         `json:"Header"`
	Item         *[]Item         `json:"Item"`
	ScheduleLine *[]ScheduleLine `json:"ScheduleLine"`
}

type Header struct {
	OrderID              int     `json:"OrderID"`
	HeaderDeliveryStatus *string `json:"HeaderDeliveryStatus"`
	IsCancelled          *bool   `json:"IsCancelled"`
	HeaderIsDeleted      *bool   `json:"HeaderIsDeleted"`
}

type Item struct {
	OrderID            int     `json:"OrderID"`
	OrderItem          int     `json:"OrderItem"`
	ItemDeliveryStatus *string `json:"ItemDeliveryStatus"`
	IsCancelled        *bool   `json:"IsCancelled"`
	ItemIsDeleted      *bool   `json:"ItemIsDeleted"`
}

type ScheduleLine struct {
	OrderID             int   `json:"OrderID"`
	OrderItem           int   `json:"OrderItem"`
	ScheduleLine        int   `json:"ScheduleLine"`
	IsCancelled         *bool `json:"IsCancelled"`
	IsMarkedForDeletion *bool `json:"IsMarkedForDeletion"`
}
