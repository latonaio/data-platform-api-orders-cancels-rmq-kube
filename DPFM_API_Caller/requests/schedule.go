package requests

type ScheduleLine struct {
	OrderID            int   `json:"OrderID"`
	OrderItem          int   `json:"OrderItem"`
	ScheduleLine       int   `json:"ScheduleLine"`
	IsCancelled        *bool `json:"IsCancelled"`
	IsMarkedForDeleted *bool `json:"IsMarkedForDeleted"`
}
