DROP TABLE IF EXISTS cleansed.weather_reykjavik, cleansed.weather_bergen, cleansed.weather_goteborg, cleansed.weather_malmo, cleansed.weather_stockholm;
CREATE TABLE IF NOT EXISTS cleansed.weather_Stockholm (
	weather_time VARCHAR(50),
	air_pressure_at_sea_level DECIMAL(5,1),
	air_temperature DECIMAL(5,1), 
	cloud_area_fraction DECIMAL(5,1), 
	relative_humidity DECIMAL(5,1), 
	wind_from_direction DECIMAL(5,1), 
	wind_speed DECIMAL(5,1), 
	symbol_code_12h VARCHAR(50), 
	symbol_code_1h VARCHAR(50), 
	precipitation_amount_1h DECIMAL(5,1), 
	symbol_code_6h VARCHAR(50), 
	precipitation_amount_6h DECIMAL(5,1)
);

CREATE TABLE IF NOT EXISTS cleansed.weather_Goteborg (
	weather_time VARCHAR(50),
	air_pressure_at_sea_level DECIMAL(5,1),
	air_temperature DECIMAL(5,1), 
	cloud_area_fraction DECIMAL(5,1), 
	relative_humidity DECIMAL(5,1), 
	wind_from_direction DECIMAL(5,1), 
	wind_speed DECIMAL(5,1), 
	symbol_code_12h VARCHAR(50), 
	symbol_code_1h VARCHAR(50), 
	precipitation_amount_1h DECIMAL(5,1), 
	symbol_code_6h VARCHAR(50), 
	precipitation_amount_6h DECIMAL(5,1)
);

CREATE TABLE IF NOT EXISTS cleansed.weather_Malmo (
	weather_time VARCHAR(50),
	air_pressure_at_sea_level DECIMAL(5,1),
	air_temperature DECIMAL(5,1), 
	cloud_area_fraction DECIMAL(5,1), 
	relative_humidity DECIMAL(5,1), 
	wind_from_direction DECIMAL(5,1), 
	wind_speed DECIMAL(5,1), 
	symbol_code_12h VARCHAR(50), 
	symbol_code_1h VARCHAR(50), 
	precipitation_amount_1h DECIMAL(5,1), 
	symbol_code_6h VARCHAR(50), 
	precipitation_amount_6h DECIMAL(5,1)
);
CREATE TABLE IF NOT EXISTS cleansed.weather_Bergen (
	weather_time VARCHAR(50),
	air_pressure_at_sea_level DECIMAL(5,1),
	air_temperature DECIMAL(5,1), 
	cloud_area_fraction DECIMAL(5,1), 
	relative_humidity DECIMAL(5,1), 
	wind_from_direction DECIMAL(5,1), 
	wind_speed DECIMAL(5,1), 
	symbol_code_12h VARCHAR(50), 
	symbol_code_1h VARCHAR(50), 
	precipitation_amount_1h DECIMAL(5,1), 
	symbol_code_6h VARCHAR(50), 
	precipitation_amount_6h DECIMAL(5,1)
);
CREATE TABLE IF NOT EXISTS cleansed.weather_Reykjavik (
	weather_time VARCHAR(50),
	air_pressure_at_sea_level DECIMAL(5,1),
	air_temperature DECIMAL(5,1), 
	cloud_area_fraction DECIMAL(5,1), 
	relative_humidity DECIMAL(5,1), 
	wind_from_direction DECIMAL(5,1), 
	wind_speed DECIMAL(5,1), 
	symbol_code_12h VARCHAR(50), 
	symbol_code_1h VARCHAR(50), 
	precipitation_amount_1h DECIMAL(5,1), 
	symbol_code_6h VARCHAR(50), 
	precipitation_amount_6h DECIMAL(5,1)
);
SELECT * FROM cleansed.weather_bergen;
