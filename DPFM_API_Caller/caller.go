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

func (c *DPFMAPICaller) AsyncOrderCancels(
	accepter []string,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (interface{}, []error) {
	var response interface{}
	if input.APIType == "cancels" {
		response = c.cancelSqlProcess(input, output, accepter, log)
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
	scheduleData := make([]dpfm_api_output_formatter.ScheduleLine, 0)
	for _, a := range accepter {
		switch a {
		case "Header":
			h, i, s := c.headerCancel(input, output, log)
			if h == nil || i == nil || s == nil {
				continue
			}
			headerData = h
			itemData = append(itemData, *i...)
			scheduleData = append(scheduleData, *s...)
		case "Item":
			i, s := c.itemCancel(input, output, log)
			if i == nil || s == nil {
				continue
			}
			itemData = append(itemData, *i...)
			scheduleData = append(scheduleData, *s...)
		case "Schedule":
			s := c.scheduleCancel(input, output, log)
			if s == nil {
				continue
			}
			scheduleData = append(scheduleData, *s...)
		}
	}

	return &dpfm_api_output_formatter.Message{
		Header:       headerData,
		Item:         &itemData,
		ScheduleLine: &scheduleData,
	}
}

func (c *DPFMAPICaller) headerCancel(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*dpfm_api_output_formatter.Header, *[]dpfm_api_output_formatter.Item, *[]dpfm_api_output_formatter.ScheduleLine) {
	sessionID := input.RuntimeSessionID
	items := c.ItemsRead(input, log)
	for i := range *items {
		t := true
		(*items)[i].IsCancelled = &t
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*items)[i], "function": "OrdersItem", "runtime_session_id": sessionID})
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

	schedules := c.ScheduleLineRead(input, log)
	for i := range *schedules {
		t := true
		(*schedules)[i].IsCancelled = &t
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*schedules)[i], "function": "OrdersItemScheduleLine", "runtime_session_id": sessionID})
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

	header := c.HeaderRead(input, log)
	header.IsCancelled = input.Orders.IsCancelled
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
	return header, items, schedules
}

func (c *DPFMAPICaller) itemCancel(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*[]dpfm_api_output_formatter.Item, *[]dpfm_api_output_formatter.ScheduleLine) {
	sessionID := input.RuntimeSessionID
	schedules := c.ScheduleLineRead(input, log)
	for _, v := range *schedules {
		t := true
		v.IsCancelled = &t
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": v, "function": "OrdersItemScheduleLine", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Order Item Schedule Line Data cannot cancel"
			return nil, nil
		}
	}

	items := make([]dpfm_api_output_formatter.Item, 0)
	for _, v := range input.Orders.Item {
		data := dpfm_api_output_formatter.Item{
			OrderID:            input.Orders.OrderID,
			OrderItem:          v.OrderItem,
			ItemDeliveryStatus: nil,
			IsCancelled:        v.IsCancelled,
			ItemIsDeleted:      nil,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": data, "function": "OrdersItem", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Order Item Data cannot cancel"
			return nil, nil
		}
	}
	return &items, schedules
}

func (c *DPFMAPICaller) scheduleCancel(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.ScheduleLine {
	sessionID := input.RuntimeSessionID
	schedules := make([]dpfm_api_output_formatter.ScheduleLine, 0)
	for _, item := range input.Orders.Item {
		for _, schedule := range item.ItemSchedulingLine {
			data := dpfm_api_output_formatter.ScheduleLine{
				OrderID:             input.Orders.OrderID,
				OrderItem:           item.OrderItem,
				ScheduleLine:        schedule.ScheduleLine,
				IsCancelled:         schedule.IsCancelled,
				IsMarkedForDeletion: nil,
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
			schedules = append(schedules, data)
		}
	}
	return &schedules
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
