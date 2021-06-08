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

COPY intermedia(Quarter, Month, Week, Product_type, Territory, Sales_Channel, Customer_type, Revenue, Cost)
FROM '/Users/roberto-j-catalan/Base de Datos 1/BD_TPE/SalesByRegion.csv'
DELIMITER ','
CSV HEADER;

--Funciones

CREATE OR REPLACE FUNCTION MedianaMargenMovil(IN date DATE, IN months INTEGER) RETURNS NUMERIC AS
$$
DECLARE
    minDate date;
    media   numeric(6, 2);

BEGIN
    IF (months = 0) THEN
        RAISE WARNING 'La cantidad de meses anteriores debe ser mayor a 0';
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


CREATE OR REPLACE FUNCTION ReporteVentas(IN years INTEGER) RETURNS VOID AS
$$
DECLARE
    currentYear  integer;
    flag         boolean;
    i            record;
    totalCost    float;
    totalRevenue float;
    totalMargin  float;
    customerCursor    CURSOR FOR SELECT definitiva.Customer_Type,
                            SUM(definitiva.Revenue)                        AS Revenue,
                            SUM(definitiva.Cost)                           AS Cost,
                            SUM(definitiva.Revenue) - SUM(definitiva.Cost) AS Margin
                     FROM definitiva
                     WHERE extract(year from definitiva.Sales_Date) = currentYear
                     GROUP BY definitiva.Customer_Type
                     ORDER BY definitiva.Customer_Type;
    productCursor   CURSOR FOR SELECT definitiva.Product_type,
                            SUM(definitiva.Revenue)                        AS Revenue,
                            SUM(definitiva.Cost)                           AS Cost,
                            SUM(definitiva.Revenue) - SUM(definitiva.Cost) AS Margin
                     FROM definitiva
                     WHERE extract(year from definitiva.Sales_Date) = currentYear
                     GROUP BY definitiva.Product_type
                     ORDER BY definitiva.Product_type;
    salesCursor     CURSOR FOR SELECT definitiva.Sales_Channel,
                            SUM(definitiva.Revenue)                        AS Revenue,
                            SUM(definitiva.Cost)                           AS Cost,
                            SUM(definitiva.Revenue) - SUM(definitiva.Cost) AS Margin
                     FROM definitiva
                     WHERE extract(year from definitiva.Sales_Date) = currentYear
                     GROUP BY definitiva.Sales_Channel
                     ORDER BY definitiva.Sales_Channel;
BEGIN
    IF (years = 0) THEN
        RAISE WARNING 'La cantidad de años debe ser mayor a 0';
        RETURN;
    end if;

    SELECT min(extract(year FROM definitiva.Sales_Date))
    INTO currentYear
    FROM definitiva;

    RAISE NOTICE '      HISTORIC SALES REPORT       ';
    RAISE NOTICE 'YEAR      CATEGORY        REVENUE     COST        MARGIN';--2 TAB ENTRE COLUMNAS

    WHILE(years > 0)
        LOOP
            totalCost := 0;
            totalRevenue := 0;
            totalMargin := 0;
            flag := TRUE;
            OPEN customerCursor;
            LOOP
                FETCH customerCursor INTO i;
                EXIT WHEN NOT FOUND;
                    IF (flag) THEN
                        RAISE NOTICE '%      Customer Type: %        %       %       %', currentYear, i.Customer_Type, CAST(i.Revenue AS integer), CAST(i.Cost AS integer), CAST(i.Margin AS integer);
                        flag := FALSE;
                    ELSE
                        RAISE NOTICE '----      Customer Type: %        %       %       %', i.Customer_Type, CAST(i.Revenue AS integer), CAST(i.Cost AS integer), CAST(i.Margin AS integer);
                    END IF;
                    totalCost := totalCost + i.Cost;
                    totalRevenue := totalRevenue + i.Revenue;
                    totalMargin := totalMargin + i.Margin;
                END LOOP;
            CLOSE customerCursor;
            OPEN productCursor;

                LOOP
                    FETCH productCursor INTO I;
                    EXIT WHEN NOT FOUND;
                    IF (flag) THEN
                        RAISE NOTICE '%      Product Type: %        %       %       %', currentYear, i.Product_type, CAST(i.Revenue AS integer), CAST(i.Cost AS integer), CAST(i.Margin AS integer);
                        flag := FALSE;
                    ELSE
                        RAISE NOTICE '----      Product Type: %        %       %       %', i.Product_type, CAST(i.Revenue AS integer), CAST(i.Cost AS integer), CAST(i.Margin AS integer);
                    END IF;

                END LOOP;
            CLOSE productCursor;
            OPEN salesCursor;
                LOOP
                    FETCH salesCursor INTO I;
                    EXIT WHEN NOT FOUND;
                    IF (flag) THEN
                        RAISE NOTICE '%      Sales Channel: %        %       %       %', currentYear, i.Sales_Channel, CAST(i.Revenue AS integer), CAST(i.Cost AS integer), CAST(i.Margin AS integer);
                        flag := FALSE;
                    ELSE
                        RAISE NOTICE '----      Sales Channel: %        %       %       %', i.Sales_Channel, CAST(i.Revenue AS integer), CAST(i.Cost AS integer), CAST(i.Margin AS integer);
                    END IF;

                END LOOP;
            CLOSE salesCursor;
            RAISE NOTICE '----       %       %       %',CAST(totalRevenue AS integer),CAST(totalCost AS integer), CAST(totalMargin AS integer);
            years := years - 1;
            currentYear := currentYear + 1;
        END LOOP;


    RETURN;
END;
$$ LANGUAGE plpgsql
    RETURNS NULL ON NULL INPUT;

SELECT ReporteVentas(2);
-- Drops
DROP TABLE intermedia;
DROP TABLE definitiva;
--------
DROP FUNCTION MedianaMargenMovil(date DATE, months INTEGER);
DROP FUNCTION ReporteVentas(years INTEGER);
DROP FUNCTION fillTable;
--------
DROP TRIGGER fillTableTrigger ON intermedia;