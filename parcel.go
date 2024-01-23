package main

import (
	"database/sql"
	"fmt"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	res, err := s.db.Exec("INSERT INTO parcel (client, status, address, created_at) VALUES (:client, :status, :adress, :created_at)",
		sql.Named("client", p.Client),
		sql.Named("status", p.Status),
		sql.Named("adress", p.Address),
		sql.Named("created_at", p.CreatedAt),
	)
	if err != nil {
		return 0, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf(`Add: не удалось добавить посылку в БД: %w`, err)
	}

	return int(id), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	row := s.db.QueryRow("SELECT * FROM parcel WHERE number = :number",
		sql.Named("number", number))

	p := Parcel{}
	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		return p, fmt.Errorf(`Get: не удалось чтение строки в БД по заданному %d: %w`, number, err)
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	rows, err := s.db.Query("SELECT * FROM parcel WHERE client = :client",
		sql.Named("client", client))
	if err != nil {
		return nil, fmt.Errorf(`GetByClient: не удалось чтение строки по клиенту %d: %w`, client, err)
	}
	defer rows.Close()

	var res []Parcel

	for rows.Next() {
		p := Parcel{}
		err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
		if err != nil {
			return res, fmt.Errorf(`GetByClient: не удалось чтение строки по клиенту %d: %w`, client, err)
		}
		res = append(res, p)
	}

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	_, err := s.db.Exec("UPDATE parcel SET status = :status WHERE number = :number",
		sql.Named("status", status),
		sql.Named("number", number),
	)
	if err != nil {
		return fmt.Errorf(`SetStatus: не удалось поменять статус у посылки %d: %w`, number, err)
	}
	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	p, err := s.Get(number)
	if err != nil {
		return err
	}
	if p.Status == ParcelStatusRegistered {
		_, err = s.db.Exec("UPDATE parcel SET address = :address WHERE number = :number",
			sql.Named("address", address),
			sql.Named("number", number),
		)
	}
	if err != nil {
		return fmt.Errorf(`SetAddress: не удалось поменять адрес у посылки %d: %w`, number, err)

	}
	return nil
}

func (s ParcelStore) Delete(number int) error {
	p, err := s.Get(number)
	if err != nil {
		return err
	}
	if p.Status == ParcelStatusRegistered {
		_, err = s.db.Exec("DELETE FROM parcel WHERE number = :number",
			sql.Named("number", number),
		)
	}
	if err != nil {
		return fmt.Errorf(`Delete: не удалось удалить посылку с номером %d: %w`, number, err)
	}
	return nil
}
