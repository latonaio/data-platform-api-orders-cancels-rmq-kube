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
		return &header, nil
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

func ConvertToSchedule(rows *sql.Rows) (*[]ScheduleLine, error) {
	defer rows.Close()
	schedules := make([]ScheduleLine, 0)
	i := 0

	for rows.Next() {
		i++
		schedule := ScheduleLine{}
		err := rows.Scan(
			&schedule.OrderID,
			&schedule.OrderItem,
			&schedule.ScheduleLine,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &schedules, err
		}

		schedules = append(schedules, schedule)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &schedules, nil
	}

	return &schedules, nil
}
