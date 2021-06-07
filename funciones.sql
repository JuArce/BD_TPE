--Creacion de tablas necesarias

CREATE TABLE intermedia
(
    Quarter       text not null,
    Month         text not null,
    Week          text not null,
    Product_type  text not null,
    Territory     text not null,
    Sales_Channel text not null,
    Customer_type text not null,
    Revenue       float,
    Cost          float,
    PRIMARY KEY (Quarter, Month, Week, Product_type, Sales_Channel, Customer_type)
);
set datestyle to YMD;
CREATE TABLE definitiva
(
    Sales_Date    date not null,
    Product_type  text not null,
    Territory     text not null,
    Sales_Channel text not null,
    Customer_Type text not null,
    Revenue       float,
    Cost          float,
    PRIMARY KEY (Sales_Date, Product_type, Sales_Channel, Customer_type)
);

--Funciones trigger

CREATE OR REPLACE FUNCTION fillTable() RETURNS TRIGGER AS
$$
DECLARE
    auxYear  text;
    auxMonth text;
    auxDay   text;
    auxDate  date;
    auxStr   text;
BEGIN
    auxYear := split_part(new.Quarter, '/', 2);
    auxMonth := split_part(new.Month, '-', 2);
    auxMonth := to_char(to_date(auxMonth, 'Mon'), 'MM');
    auxDay := split_part(new.Week, '-', 1);

    case when auxDay = 'W1' then auxDay := '01';
        when auxDay = 'W2' then auxDay := '08';
        when auxDay = 'W3' then auxDay := '15';
        when auxDay = 'W4' then auxDay := '22';
        when auxDay = 'W5' then auxDay := '29';
        end case;

    auxStr := concat(auxYear, auxMonth, auxDay);
    auxDate := to_date(auxStr, 'YYYYMMDD');

    INSERT INTO definitiva(Sales_Date, Product_type, Territory, Sales_Channel, Customer_type, Revenue, Cost)
    VALUES (auxDate, new.Product_type, new.Territory, new.Sales_Channel, new.Customer_type, new.Revenue, new.Cost);
    RETURN new;
END

$$ LANGUAGE plpgsql;

CREATE TRIGGER fillTableTrigger
    AFTER INSERT
    ON intermedia
    FOR EACH ROW
EXECUTE PROCEDURE fillTable();

--Funciones

CREATE OR REPLACE FUNCTION MedianaMargenMovil(IN date DATE, IN months INTEGER) RETURNS NUMERIC AS
$$
DECLARE
    minDate date;
    media   numeric(6, 2);

BEGIN
    IF (months = 0) THEN
        RAISE NOTICE 'La cantidad de meses anteriores debe ser mayor a 0';
        RETURN NULL;
    end if;

    minDate := date - interval '1 month' * months;

    SELECT percentile_cont(0.5) within group (order by (Revenue - Cost))
    INTO media
    FROM definitiva
    WHERE sales_date > minDate
      AND sales_date <= date;

    RETURN media;
END;
$$ LANGUAGE plpgsql;


-- Debe dar 1067.33
SELECT MedianaMargenMovil(to_date('2012-11-01', 'YYYY-MM-DD'), 3);

-- Debe dar 1096.4
SELECT MedianaMargenMovil(to_date('2012-11-01', 'YYYY-MM-DD'), 4);

-- Debe dar 1155.5
SELECT MedianaMargenMovil(to_date('2011-09-01', 'YYYY-MM-DD'), 5);

-- Debe dar mensaje de error
SELECT MedianaMargenMovil(to_date('2012-11-01', 'YYYY-MM-DD'), 0);
