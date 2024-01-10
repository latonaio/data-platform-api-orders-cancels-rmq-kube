package dpfm_api_output_formatter

import (
	"database/sql"
	"fmt"
)

func ConvertToHeader(rows *sql.Rows) (*Header, error) {
	defer rows.Close()
	header := Header{}
	i := 0

	for rows.Next() {
		i++
		err := rows.Scan(
			&header.OrderID,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &header, err
		}

	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return nil, nil
	}

	return &header, nil
}

func ConvertToItem(rows *sql.Rows) (*[]Item, error) {
	defer rows.Close()
	items := make([]Item, 0)
	i := 0

	for rows.Next() {
		i++
		item := Item{}
		err := rows.Scan(
			&item.OrderID,
			&item.OrderItem,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &items, err
		}

		items = append(items, item)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &items, nil
	}

	return &items, nil
}

func ConvertToItemScheduleLine(rows *sql.Rows) (*[]ItemScheduleLine, error) {
	defer rows.Close()
	itemScheduleLines := make([]ItemScheduleLine, 0)
	i := 0

	for rows.Next() {
		itemScheduleLine := ItemScheduleLine{}
		i++
		err := rows.Scan(
			&schedule.OrderID,
			&schedule.OrderItem,
			&schedule.ScheduleLine,
			&schedule.Product,
			&schedule.StockConfirmationBusinessPartner,
			&schedule.StockConfirmationPlant,
			&schedule.StockConfirmationPlantBatch,
			&schedule.RequestedDeliveryDate,
			&schedule.ConfirmedOrderQuantityByPDTAvailCheckInBaseUnit,
			&schedule.IsCancelled,
			&schedule.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &schedules, err
		}

		itemScheduleLines = append(itemScheduleLines, itemScheduleLine)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &itemScheduleLines, nil
	}

	return &itemScheduleLines, nil
}

func ConvertToProductStockAvailability(rows *sql.Rows) (*ProductStock, error) {
	defer rows.Close()
	productStock := ProductStock{}
	i := 0

	for rows.Next() {
		i++
		err := rows.Scan(
			&productStock.Product,
			&productStock.BusinessPartner,
			&productStock.Plant,
			&productStock.ProductStockAvailabilityDate,
			&productStock.AvailableProductStock,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &productStock, err
		}

	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &productStock, nil
	}

	return &productStock, nil
}

func ConvertToProductStockAvailabilityByBatch(rows *sql.Rows) (*ProductStock, error) {
	defer rows.Close()
	productStock := ProductStock{}
	i := 0

	for rows.Next() {
		i++
		err := rows.Scan(
			&productStock.Product,
			&productStock.BusinessPartner,
			&productStock.Plant,
			&productStock.Batch,
			&productStock.ProductStockAvailabilityDate,
			&productStock.AvailableProductStock,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &productStock, err
		}

	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &productStock, nil
	}

	return &productStock, nil
}
