package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-orders-cancels-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-orders-cancels-rmq-kube/DPFM_API_Output_Formatter"

	"fmt"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) HeaderRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.Header {
	where := fmt.Sprintf("WHERE header.OrderID = %d ", input.Orders.OrderID)
	if input.Orders.HeaderDeliveryStatus != nil {
		where = fmt.Sprintf("%s \n AND HeaderDeliveryStatus = %s ", where, *input.Orders.HeaderDeliveryStatus)
	}
	where = fmt.Sprintf("%s \n AND ( header.Buyer = %d OR header.Seller = %d ) ", where, input.BusinessPartner, input.BusinessPartner)
	rows, err := c.db.Query(
		`SELECT 
			header.OrderID
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_orders_header_data as header ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) ItemsRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Item {
	where := fmt.Sprintf("WHERE item.OrderID IS NOT NULL\nAND header.OrderID = %d", input.Orders.OrderID)
	where = fmt.Sprintf("%s\nAND ( header.Buyer = %d OR header.Seller = %d ) ", where, input.BusinessPartner, input.BusinessPartner)
	rows, err := c.db.Query(
		`SELECT 
			item.OrderID, item.OrderItem
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_orders_item_data as item
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_orders_header_data as header
		ON header.OrderID = item.OrderID ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToItem(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) ScheduleLineRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.ScheduleLine {
	where := fmt.Sprintf("WHERE schedule.OrderID IS NOT NULL\nAND header.OrderID = %d", input.Orders.OrderID)
	where = fmt.Sprintf("%s\nAND ( header.Buyer = %d OR header.Seller = %d ) ", where, input.BusinessPartner, input.BusinessPartner)
	rows, err := c.db.Query(
		`SELECT 
			schedule.OrderID, schedule.OrderItem, schedule.ScheduleLine, schedule.Product, schedule.StockConfirmationBusinessPartner,
			schedule.StockConfirmationPlant, schedule.StockConfirmationPlantBatch, schedule.RequestedDeliveryDate,
			schedule.ConfirmedOrderQuantityByPDTAvailCheckInBaseUnit,	schedule.IsCancelled, schedule.IsMarkedForDeletion
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_orders_item_schedule_line_data as schedule
		INNER JOIN DataPlatformMastersAndTransactionsMysqlKube.data_platform_orders_header_data as header
		ON header.OrderID = schedule.OrderID ` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToSchedule(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) ProductStockAvailabilityRead(
	schedule dpfm_api_output_formatter.ScheduleLine,
	log *logger.Logger,
) *dpfm_api_output_formatter.ProductStock {
	args := make([]interface{}, 0)

	args = append(args, schedule.Product, schedule.StockConfirmationBusinessPartner, schedule.StockConfirmationPlant, schedule.RequestedDeliveryDate)

	rows, err := c.db.Query(
		`SELECT Product, BusinessPartner, Plant, ProductStockAvailabilityDate, AvailableProductStock
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_product_stock_product_stock_availability_data
		WHERE (Product, BusinessPartner, Plant , ProductStockAvailabilityDate) = (?, ?, ?, ?);`, args...,
	)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToProductStockAvailability(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) ProductStockAvailabilityByBatchRead(
	schedule dpfm_api_output_formatter.ScheduleLine,
	log *logger.Logger,
) *dpfm_api_output_formatter.ProductStock {
	args := make([]interface{}, 0)

	args = append(args, schedule.Product, schedule.StockConfirmationBusinessPartner, schedule.StockConfirmationPlant, *schedule.StockConfirmationPlantBatch, schedule.RequestedDeliveryDate)

	rows, err := c.db.Query(
		`SELECT Product, BusinessPartner, Plant, Batch, ProductStockAvailabilityDate, AvailableProductStock
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_product_stock_product_stock_avail_by_btch
		WHERE (Product, BusinessPartner, Plant, Batch, ProductStockAvailabilityDate) = (?, ?, ?, ?, ?);`, args...,
	)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToProductStockAvailabilityByBatch(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
