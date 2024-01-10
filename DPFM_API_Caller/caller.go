package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-orders-cancels-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-orders-cancels-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-orders-cancels-rmq-kube/config"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	database "github.com/latonaio/golang-mysql-network-connector"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
	"golang.org/x/xerrors"
)

type DPFMAPICaller struct {
	ctx  context.Context
	conf *config.Conf
	rmq  *rabbitmq.RabbitmqClient
	db   *database.Mysql
}

func NewDPFMAPICaller(
	conf *config.Conf, rmq *rabbitmq.RabbitmqClient, db *database.Mysql,
) *DPFMAPICaller {
	return &DPFMAPICaller{
		ctx:  context.Background(),
		conf: conf,
		rmq:  rmq,
		db:   db,
	}
}

func (c *DPFMAPICaller) AsyncCancels(
	accepter []string,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (interface{}, []error) {
	var response interface{}
	switch input.APIType {
	case "cancels":
		response = c.cancelSqlProcess(input, output, accepter, log)
	default:
		log.Error("unknown api type %s", input.APIType)
	}
	return response, nil
}

func (c *DPFMAPICaller) cancelSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	log *logger.Logger,
) *dpfm_api_output_formatter.Message {
	var headerData *dpfm_api_output_formatter.Header
	itemData := make([]dpfm_api_output_formatter.Item, 0)
	itemScheduleLineData := make([]dpfm_api_output_formatter.ItemScheduleLine, 0)
	productStockData := make([]dpfm_api_output_formatter.ProductStock, 0)
	for _, a := range accepter {
		switch a {
		case "Header":
			h, i, s, p := c.headerCancel(input, output, log)
			headerData = h
			if h == nil || i == nil || s == nil {
				continue
			}
			itemData = append(itemData, *i...)
			itemScheduleLineData = append(itemScheduleLineData, *s...)
			productStockData = append(productStockData, *p...)
		case "Item":
			i, s, p := c.itemCancel(input, output, log)
			if i == nil || s == nil {
				continue
			}
			itemData = append(itemData, *i...)
			itemScheduleLineData = append(itemScheduleLineData, *s...)
			productStockData = append(productStockData, *p...)
		case "ItemScheduleLine":
			s := c.itemScheduleLineCancel(input, output, log)
			if s == nil {
				continue
			}
			itemScheduleLineData = append(itemScheduleLineData, *s...)
		}
	}

	return &dpfm_api_output_formatter.Message{
		Header:       		headerData,
		Item:         		&itemData,
		ItemScheduleLine:	&itemScheduleLineData,
		ProductStock: 		&productStockData,
	}
}

func (c *DPFMAPICaller) headerCancel(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*dpfm_api_output_formatter.Header, *[]dpfm_api_output_formatter.Item, *[]dpfm_api_output_formatter.ItemScheduleLine, *[]dpfm_api_output_formatter.ProductStock) {
	sessionID := input.RuntimeSessionID

	header := c.HeaderRead(input, log)
	if header == nil {
		return nil, nil, nil, nil
	}
	header.IsCancelled = input.Header.IsCancelled
	res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "OrdersHeader", "runtime_session_id": sessionID})
	if err != nil {
		err = xerrors.Errorf("rmq error: %w", err)
		log.Error("%+v", err)
		return nil, nil, nil, nil
	}
	res.Success()
	if !checkResult(res) {
		output.SQLUpdateResult = getBoolPtr(false)
		output.SQLUpdateError = "Header Data cannot cancel"
		return nil, nil, nil, nil
	}
	// headerのキャンセルが取り消された時は子に影響を与えない
	if !*header.IsCancelled {
		return header, nil, nil, nil
	}

	items := c.ItemsRead(input, log)
	for i := range *items {
		(*items)[i].IsCancelled = input.Header.IsCancelled
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*items)[i], "function": "OrdersItem", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Order Item Data cannot cancel"
			return nil, nil, nil, nil
		}
	}

	itemScheduleLines := c.ItemScheduleLineRead(input, log)
	productStocks := make([]dpfm_api_output_formatter.ProductStock, 0)
	for i := range *itemScheduleLines {
		confirmedOrderQuantityByPDTAvailCheckInBaseUnit := float32(0)
		productStock := new(dpfm_api_output_formatter.ProductStock)
		if *input.Header.IsCancelled {
			productStock, confirmedOrderQuantityByPDTAvailCheckInBaseUnit = c.releaseInventoryReservation(input, output, (*itemScheduleLines)[i], log)
		} else if !*input.Header.IsCancelled {
			productStock, confirmedOrderQuantityByPDTAvailCheckInBaseUnit = c.inventoryReservation(input, output, (*itemScheduleLines)[i], log)
		}
		productStocks = append(productStocks, *productStock)

		(*itemScheduleLines)[i].ConfirmedOrderQuantityByPDTAvailCheckInBaseUnit = confirmedOrderQuantityByPDTAvailCheckInBaseUnit
		(*itemScheduleLines)[i].IsCancelled = input.Header.IsCancelled
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*itemScheduleLines)[i], "function": "OrdersItemScheduleLine", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Order Item Schedule Line Data cannot cancel"
			return nil, nil, nil, nil
		}
	}

	return header, items, itemScheduleLines, &productStocks
}

func (c *DPFMAPICaller) itemCancel(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*[]dpfm_api_output_formatter.Item, *[]dpfm_api_output_formatter.ItemScheduleLine, *[]dpfm_api_output_formatter.ProductStock) {
	sessionID := input.RuntimeSessionID
	itemScheduleLines := c.ItemScheduleLineRead(input, log)
	item := input.Header.Item[0]
	productStocks := make([]dpfm_api_output_formatter.ProductStock, 0)
	for _, v := range *itemScheduleLines {
		confirmedOrderQuantityByPDTAvailCheckInBaseUnit := float32(0)
		productStock := new(dpfm_api_output_formatter.ProductStock)
		ordersCancel := false
		if input.Header.IsCancelled != nil {
			ordersCancel = *input.Header.IsCancelled
		}
		if ordersCancel {
			productStock, confirmedOrderQuantityByPDTAvailCheckInBaseUnit = c.releaseInventoryReservation(input, output, v, log)
		} else if !ordersCancel {
			productStock, confirmedOrderQuantityByPDTAvailCheckInBaseUnit = c.inventoryReservation(input, output, v, log)
		}
		productStocks = append(productStocks, *productStock)

		v.ConfirmedOrderQuantityByPDTAvailCheckInBaseUnit = confirmedOrderQuantityByPDTAvailCheckInBaseUnit
		v.IsCancelled = item.IsCancelled
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": v, "function": "OrdersItemScheduleLine", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Order Item Schedule Line Data cannot cancel"
			return nil, nil, nil
		}
	}

	items := make([]dpfm_api_output_formatter.Item, 0)
	for _, v := range input.Header.Item {
		data := dpfm_api_output_formatter.Item{
			OrderID:            input.Header.OrderID,
			OrderItem:          v.OrderItem,
			ItemDeliveryStatus: nil,
			IsCancelled:        v.IsCancelled,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "OrdersItem", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Order Item Data cannot cancel"
			return nil, nil, nil
		}
	}

	// itemがキャンセル取り消しされた場合、headerのキャンセルも取り消す
	if !*input.Header.Item[0].IsCancelled {
		header := c.HeaderRead(input, log)
		header.IsCancelled = input.Header.Item[0].IsCancelled
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": header, "function": "OrdersHeader", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Header Data cannot cancel"
			return nil, nil, nil
		}
	}

	return &items, itemScheduleLines, &productStocks
}

func (c *DPFMAPICaller) itemScheduleLineCancel(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.ItemScheduleLine {
	sessionID := input.RuntimeSessionID
	itemScheduleLines := make([]dpfm_api_output_formatter.ItemScheduleLine, 0)
	for _, item := range input.Header.Item {
		for _, itemScheduleLine := range item.ItemSchedulingLine {
			data := dpfm_api_output_formatter.ItemScheduleLine{
				OrderID:      input.Header.OrderID,
				OrderItem:    item.OrderItem,
				ScheduleLine: itemScheduleLine.ScheduleLine,
				IsCancelled:  itemScheduleLine.IsCancelled,
			}

			res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "OrdersItemScheduleLine", "runtime_session_id": sessionID})
			if err != nil {
				err = xerrors.Errorf("rmq error: %w", err)
				log.Error("%+v", err)
				return nil
			}
			res.Success()
			if !checkResult(res) {
				output.SQLUpdateResult = getBoolPtr(false)
				output.SQLUpdateError = "Order Item Schedule Line Data cannot cancel"
				return nil
			}
			itemScheduleLines = append(itemScheduleLines, data)
		}
	}
	return &itemScheduleLines
}

func (c *DPFMAPICaller) releaseInventoryReservation(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	itemScheduleLine dpfm_api_output_formatter.ItemScheduleLine,
	log *logger.Logger,
) (*dpfm_api_output_formatter.ProductStock, float32) {
	sessionID := input.RuntimeSessionID

	if itemScheduleLine.StockConfirmationPlantBatch == nil {
		productStock := c.ProductStockAvailabilityRead(itemScheduleLine, log)
		availableProductStock := productStock.AvailableProductStock
		confirmedOrderQuantityByPDTAvailCheckInBaseUnit := itemScheduleLine.ConfirmedOrderQuantityByPDTAvailCheckInBaseUnit
		recalculatedAvailableProductStock := availableProductStock + confirmedOrderQuantityByPDTAvailCheckInBaseUnit

		data := dpfm_api_output_formatter.ProductStock{
			Product:                      productStock.Product,
			BusinessPartner:              productStock.BusinessPartner,
			Plant:                        productStock.Plant,
			ProductStockAvailabilityDate: productStock.ProductStockAvailabilityDate,
			AvailableProductStock:        recalculatedAvailableProductStock,
		}

		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "ProductStockAvailability", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, 0
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Product Stock Availability Data cannot update"
			return nil, 0
		}

		return &data, 0
	} else {
		productStock := c.ProductStockAvailabilityByBatchRead(itemScheduleLine, log)
		availableProductStock := productStock.AvailableProductStock
		confirmedOrderQuantityByPDTAvailCheckInBaseUnit := itemScheduleLine.ConfirmedOrderQuantityByPDTAvailCheckInBaseUnit
		recalculatedAvailableProductStock := availableProductStock + confirmedOrderQuantityByPDTAvailCheckInBaseUnit

		data := dpfm_api_output_formatter.ProductStock{
			Product:                      productStock.Product,
			BusinessPartner:              productStock.BusinessPartner,
			Plant:                        productStock.Plant,
			Batch:                        productStock.Batch,
			ProductStockAvailabilityDate: productStock.ProductStockAvailabilityDate,
			AvailableProductStock:        recalculatedAvailableProductStock,
		}

		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "ProductStockAvailabilityByBatch", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, 0
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Product Stock Availability By Batch Data cannot update"
			return nil, 0
		}

		return &data, 0
	}
}

func (c *DPFMAPICaller) inventoryReservation(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	itemScheduleLine dpfm_api_output_formatter.ItemScheduleLine,
	log *logger.Logger,
) (*dpfm_api_output_formatter.ProductStock, float32) {
	sessionID := input.RuntimeSessionID

	if itemScheduleLine.StockConfirmationPlantBatch == nil {
		productStock := c.ProductStockAvailabilityRead(itemScheduleLine, log)
		availableProductStock := productStock.AvailableProductStock
		confirmedOrderQuantityByPDTAvailCheckInBaseUnit := itemScheduleLine.ConfirmedOrderQuantityByPDTAvailCheckInBaseUnit
		recalculatedAvailableProductStock := float32(0)
		if availableProductStock >= confirmedOrderQuantityByPDTAvailCheckInBaseUnit {
			recalculatedAvailableProductStock = availableProductStock - confirmedOrderQuantityByPDTAvailCheckInBaseUnit
		}

		data := dpfm_api_output_formatter.ProductStock{
			Product:                      productStock.Product,
			BusinessPartner:              productStock.BusinessPartner,
			Plant:                        productStock.Plant,
			ProductStockAvailabilityDate: productStock.ProductStockAvailabilityDate,
			AvailableProductStock:        recalculatedAvailableProductStock,
		}

		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "ProductStockAvailability", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, 0
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Product Stock Availability Data cannot update"
			return nil, 0
		}

		if availableProductStock >= recalculatedAvailableProductStock {
			return productStock, confirmedOrderQuantityByPDTAvailCheckInBaseUnit
		}

		return &data, availableProductStock
	} else {
		productStock := c.ProductStockAvailabilityByBatchRead(itemScheduleLine, log)
		availableProductStock := productStock.AvailableProductStock
		confirmedOrderQuantityByPDTAvailCheckInBaseUnit := itemScheduleLine.ConfirmedOrderQuantityByPDTAvailCheckInBaseUnit
		recalculatedAvailableProductStock := float32(0)
		if availableProductStock >= confirmedOrderQuantityByPDTAvailCheckInBaseUnit {
			recalculatedAvailableProductStock = availableProductStock - confirmedOrderQuantityByPDTAvailCheckInBaseUnit
		}

		data := dpfm_api_output_formatter.ProductStock{
			Product:                      productStock.Product,
			BusinessPartner:              productStock.BusinessPartner,
			Plant:                        productStock.Plant,
			Batch:                        productStock.Batch,
			ProductStockAvailabilityDate: productStock.ProductStockAvailabilityDate,
			AvailableProductStock:        recalculatedAvailableProductStock,
		}

		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "ProductStockAvailabilityByBatch", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, 0
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Product Stock Availability By Batch Data cannot update"
			return nil, 0
		}

		if availableProductStock >= recalculatedAvailableProductStock {
			return productStock, confirmedOrderQuantityByPDTAvailCheckInBaseUnit
		}
		return &data, availableProductStock
	}
}

func checkResult(msg rabbitmq.RabbitmqMessage) bool {
	data := msg.Data()
	d, ok := data["result"]
	if !ok {
		return false
	}
	result, ok := d.(string)
	if !ok {
		return false
	}
	return result == "success"
}

func getBoolPtr(b bool) *bool {
	return &b
}
