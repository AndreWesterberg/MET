CREATE TABLE IF NOT EXISTS cleansed.weather (
	weather_time VARCHAR(50),
	air_pressure_at_sea_level VARCHAR(50),
	air_temperature VARCHAR(50), 
	cloud_area_fraction VARCHAR(50), 
	relative_humidity VARCHAR(50), 
	wind_from_direction VARCHAR(50), 
	wind_speed VARCHAR(50), 
	symbol_code_12h VARCHAR(50), 
	symbol_code_1h VARCHAR(50), 
	precipitation_amount_1h VARCHAR(50), 
	symbol_code_6h VARCHAR(50), 
	precipitation_amount_6h VARCHAR(50)
);
SELECT * FROM cleansed.weather;