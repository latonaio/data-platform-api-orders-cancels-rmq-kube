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
	Header       		*Header         	`json:"Header"`
	Item         		*[]Item         	`json:"Item"`
	ItemScheduleLine	*[]ItemScheduleLine `json:"ItemScheduleLine"`
	ProductStock 		*[]ProductStock 	`json:"ProductStock"`
}

type Header struct {
	OrderID              int     `json:"OrderID"`
	HeaderDeliveryStatus *string `json:"HeaderDeliveryStatus"`
	IsCancelled          *bool   `json:"IsCancelled"`
}

type Item struct {
	OrderID            int     `json:"OrderID"`
	OrderItem          int     `json:"OrderItem"`
	ItemDeliveryStatus *string `json:"ItemDeliveryStatus"`
	IsCancelled        *bool   `json:"IsCancelled"`
}

type ItemScheduleLine struct {
	OrderID                                         int     `json:"OrderID"`
	OrderItem                                       int     `json:"OrderItem"`
	ScheduleLine                                    int     `json:"ScheduleLine"`
	Product                                         string  `json:"Product"`
	StockConfirmationBusinessPartner                int     `json:"StockConfirmationBusinessPartner"`
	StockConfirmationPlant                          string  `json:"StockConfirmationPlant"`
	StockConfirmationPlantBatch                     *string `json:"StockConfirmationPlantBatch"`
	RequestedDeliveryDate                           *string `json:"RequestedDeliveryDate"`
	ConfirmedOrderQuantityByPDTAvailCheckInBaseUnit float32 `json:"ConfirmedOrderQuantityByPDTAvailCheckInBaseUnit"`
	IsCancelled                                     *bool   `json:"IsCancelled"`
}

type ProductStock struct {
	Product                      string  `json:"Product"`
	BusinessPartner              int     `json:"BusinessPartner"`
	Plant                        string  `json:"Plant"`
	Batch                        string  `json:"Batch"`
	ProductStockAvailabilityDate string  `json:"ProductStockAvailabilityDate"`
	AvailableProductStock        float32 `json:"AvailableProductStock"`
}
