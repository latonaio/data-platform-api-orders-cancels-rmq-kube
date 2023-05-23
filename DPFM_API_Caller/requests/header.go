package requests

type Header struct {
	OrderID              int     `json:"OrderID"`
	HeaderDeliveryStatus *string `json:"HeaderDeliveryStatus"`
	IsCancelled          *bool   `json:"IsCancelled"`
}
