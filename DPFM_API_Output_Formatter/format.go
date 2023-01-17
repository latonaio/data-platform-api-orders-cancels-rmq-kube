package dpfm_api_output_formatter

import (
	"data-platform-api-orders-cancels-rmq-kube/sub_func_complementer"
)

func ConvertToHeader(subfuncSDC *sub_func_complementer.SDC) *Header {
	data := subfuncSDC.Message.Header

	header := &Header{
		OrderID:              data.OrderID,
		HeaderDeliveryStatus: data.HeaderDeliveryStatus,
		HeaderIsCancelled:    data.HeaderIsCancelled,
		HeaderIsDeleted:      data.HeaderIsDeleted,
	}

	return header
}

func ConvertToItem(subfuncSDC *sub_func_complementer.SDC) *[]Item {
	var item []Item

	for _, data := range *subfuncSDC.Message.Item {
		item = append(item, Item{
			OrderID:            data.OrderID,
			OrderItem:          data.OrderItem,
			ItemDeliveryStatus: data.ItemDeliveryStatus,
			ItemIsCancelled:    data.ItemIsCancelled,
			ItemIsDeleted:      data.ItemIsDeleted,
		})
	}

	return &item
}
