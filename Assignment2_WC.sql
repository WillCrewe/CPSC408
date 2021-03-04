--Will Crewe
--CPSC408
--crewe@chapman.edu
--3/4/21

--Question 1
Create Table Player (
    pId SMALLINT Unsigned NOT NULL,
    Name varchar(30) NOT NULL,
    teamName varchar(30),
    PRIMARY KEY (pId)
);

--Question 2
Alter Table Player
Add Age UNSIGNED TINYINT;

--Question 3
INSERT into Player VALUES (1,'Player 1','Team A',23);
INSERT into Player (pId, Name, teamName) VALUES (2, 'Player 2', 'Team A');
Insert into Player VALUES (3, 'Player 3', 'Team B', 28);
INSERT into Player (pId, Name, teamName) VALUES (4, 'Player 4', 'Team B');

--Question 4
DELETE from Player Where pId = 2;

--Question 5
Update Player
Set Age = 25
where Age is null;

--Question 6
Select count(*), AVG(Age)
from Player;

--Question 7
Drop Table Player;

--Question 8
Select AVG(Total)
From Invoice
Where BillingCountry = 'Brazil';

--Question 9
Select BillingCity, Avg(Total)
From Invoice
Where BillingCountry = 'Brazil'
GROUP BY BillingCity;

--Question 10
Select Album.Title as Title, Count(Track.AlbumId) as Count
From Album, Track
WHERE Album.AlbumId = Track.AlbumId
GROUP BY Album.Title
Having Count > 20
Order BY Count desc;

--Question 11
Select Count(*)
From Invoice
Where InvoiceDate BETWEEN '2010-01-01' and '2010-12-31';

--Question 12
SELECT BillingCountry as Country, Count(DISTINCT(BillingCity)) as Count
From Invoice
GROUP BY BillingCountry
ORDER BY Count desc;

--Question 13
Select Album.Title as AlbumName, Track.Name TrackName, MediaType.Name as MediaType
From Track, Album, MediaType
Where Track.MediaTypeId = MediaType.MediaTypeId and Track.AlbumId = Album.AlbumId

--Question 14
Select Count(*)
From Employee, Customer, Invoice
Where Employee.EmployeeId = Customer.SupportRepId and Invoice.CustomerId = Invoice.CustomerId
AND Customer.CustomerId =
      (select Invoice.CustomerId
          From Invoice, Employee
              Where Employee.FirstName = 'Jane' and Employee.LastName = 'Peacock');







