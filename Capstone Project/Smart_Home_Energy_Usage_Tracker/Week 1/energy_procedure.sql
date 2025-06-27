USE SmartHomeDB;

CREATE PROCEDURE sp_daily_room_usage
    @date DATE
AS
BEGIN
    SELECT 
        r.name AS room_name,
        SUM(e.energy_kwh) AS total_kwh
    FROM energy_logs e
    JOIN devices d ON e.device_id = d.device_id
    JOIN rooms r ON d.room_id = r.room_id
    WHERE CAST(e.timestamp AS DATE) = @date
    GROUP BY r.name;
END;
