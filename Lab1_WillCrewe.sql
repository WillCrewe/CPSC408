select FirstName, LastName, Email from Employee;
select * from Artist;
select * from Employee where Title like "%manager";
select max(Total) from Invoice;
select min(Total) from Invoice;
select BillingAddress, BillingCity, BillingCity, BillingPostalCode, Total from Invoice where BillingCountry = 'Germany';
select BillingAddress, BillingCity, BillingCity, BillingPostalCode, Total from Invoice where Total > 14.99 and Total < 25.01;
select distinct BillingCountry from Invoice;
select FirstName, LastName, CustomerId, Country from Customer where Country != 'USA';
select * from Customer where Country = 'Brazil';
select InvoiceLineId,Name from Track natural join InvoiceLine order by Name;

