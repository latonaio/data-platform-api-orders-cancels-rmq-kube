package requests

type Header struct {
	OrderID              int     `json:"OrderID"`
	HeaderDeliveryStatus *string `json:"HeaderDeliveryStatus"`
	HeaderIsCancelled    *bool   `json:"HeaderIsCancelled"`
	HeaderIsDeleted      *bool   `json:"HeaderIsDeleted"`
}
