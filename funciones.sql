--Creacion de tablas necesarias

CREATE TABLE intermedia(
    Quarter text not null,
    Month text not null,
    Week text not null,
    Product_type text not null,
    Territory text not null,
    Sales_Channel text not null,
    Customer_type text not null,
    Revenue float,
    Cost float,
    PRIMARY KEY (Quarter,Month,Week,Product_type,Sales_Channel,Customer_type )
);
set datestyle to YMD;
CREATE TABLE definitiva(
    Sales_Date date not null,
    Product_type text not null,
    Territory text not null,
    Sales_Channel text not null,
    Customer_Type text not null,
    Revenue float,
    Cost float,
    PRIMARY KEY (Sales_Date,Product_type,Sales_Channel,Customer_type )
);

--Funciones trigger

