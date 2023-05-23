package requests

type Item struct {
	OrderID            int     `json:"OrderID"`
	OrderItem          int     `json:"OrderItem"`
	ItemDeliveryStatus *string `json:"ItemDeliveryStatus"`
	IsCancelled        *bool   `json:"IsCancelled"`
}
