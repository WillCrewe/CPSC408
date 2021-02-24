--Will Crewe
--CPSC 408
--crewe@chapman.edu
--2/23/21
--Using the aggregate function 'Max' to find max total
select max(Total) from Invoice;

--Sorting the Totals then limiting the output to 1 to show max value
select Total from Invoice ORDER BY Total DESC Limit 1;

--Selecting the amount of rows there are with the specific media types
--Which yields the amount of tracks
select MediaType.Name,Count(*) as tracks
from Track,MediaType
where Track.MediaTypeId = MediaType.MediaTypeId
GROUP BY MediaType.Name;

--adding the condition of sorting the track values in descending order
--from previous query
select MediaType.Name,Count(*) as tracks
from Track,MediaType
where Track.MediaTypeId = MediaType.MediaTypeId
GROUP BY MediaType.Name
ORDER BY COUNT(*) DESC;

--Adding the condition of having more than 200 tracks from previous query
select MediaType.Name,Count(*) as tracks
from Track,MediaType
where Track.MediaTypeId = MediaType.MediaTypeId
GROUP BY MediaType.Name
Having COUNT(*) > 200
ORDER BY COUNT(*) DESC;

--Selecting the amount of tracks by Distinct(unique) artists
--Then joining the three tables using keys
--Then using condition of the artist name starting with A
select COUNT(*) Tracks,COUNT(DISTINCT Artist.Name) Artists
from Track,Artist,Album
where Artist.ArtistId = Album.ArtistId
and Track.AlbumId = Album.AlbumId
and Artist.Name like '%A';

--||' '|| concatenation of columns
select FirstName||' '||LastName FullName, BirthDate,
       --Case to determine decade using Date type
       case
            when BirthDate between '1940-01-01' and '1950-01-01' then '40S'
            when BirthDate between '1950-01-01' and '1960-01-01' then '50S'
            when BirthDate between '1960-01-01' and '1970-01-01' then '60S'
                else '70S'
                end as decade
from Employee;
