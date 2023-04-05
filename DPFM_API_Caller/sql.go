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
	where = fmt.Sprintf("%s \n AND ( header.Buyer = %d OR header.Seller = %d ) ", where, input.BusinessPartner, input.BusinessPartner)
	where = fmt.Sprintf("%s \n AND ( header.HeaderDeliveryStatus, header.IsCancelled, header.IsMarkedForDeletion ) = ( 'NP', false, false ) ", where)
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
	where = fmt.Sprintf("%s\nAND ( item.ItemDeliveryStatus, item.IsCancelled, item.IsMarkedForDeletion) = ('NP', false, false) ", where)
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
	where = fmt.Sprintf("%s\nAND (schedule.IsCancelled, schedule.IsMarkedForDeletion) = (false, false) ", where)
	rows, err := c.db.Query(
		`SELECT 
			schedule.OrderID, schedule.OrderItem, schedule.ScheduleLine
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
