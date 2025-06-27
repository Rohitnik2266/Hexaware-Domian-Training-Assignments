CREATE DATABASE SmartHomeDB;

USE SmartHomeDB;

CREATE TABLE rooms (
    room_id INT PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE devices (
    device_id INT PRIMARY KEY,
    room_id INT FOREIGN KEY REFERENCES rooms(room_id),
    device_name VARCHAR(100),
    type VARCHAR(50),
    status VARCHAR(10)
);

CREATE TABLE energy_logs (
    log_id INT PRIMARY KEY,
    device_id INT FOREIGN KEY REFERENCES devices(device_id),
    timestamp DATETIME,
    energy_kwh FLOAT
);
