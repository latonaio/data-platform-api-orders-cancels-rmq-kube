package requests

type Item struct {
	OrderID            int     `json:"OrderID"`
	OrderItem          int     `json:"OrderItem"`
	ItemDeliveryStatus *string `json:"ItemDeliveryStatus"`
	ItemIsCancelled    *bool   `json:"ItemIsCancelled"`
	ItemIsDeleted      *bool   `json:"ItemIsDeleted"`
}
