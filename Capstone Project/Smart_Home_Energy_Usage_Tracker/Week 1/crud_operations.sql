USE SmartHomeDB;

INSERT INTO rooms VALUES (1, 'Living Room'), (2, 'Kitchen'), (3, 'Bedroom');

INSERT INTO devices VALUES
(101, 1, 'Smart Bulb', 'Lighting', 'On'),
(102, 1, 'Smart TV', 'Entertainment', 'On'),
(103, 2, 'Refrigerator', 'Appliance', 'On'),
(104, 2, 'Microwave', 'Appliance', 'Off'),
(105, 3, 'Heater', 'Climate', 'Off');

INSERT INTO energy_logs VALUES
(1, 101, '2025-06-26 09:00:00', 0.2),
(2, 101, '2025-06-26 12:00:00', 0.3),
(3, 102, '2025-06-26 10:00:00', 0.5),
(4, 103, '2025-06-26 11:00:00', 0.8),
(5, 104, '2025-06-26 13:00:00', 0.6),
(6, 105, '2025-06-26 14:00:00', 1.2),
(7, 103, '2025-06-26 15:00:00', 0.9),
(8, 102, '2025-06-26 16:00:00', 0.4),
(9, 101, '2025-06-26 17:00:00', 0.3),
(10, 105, '2025-06-26 18:00:00', 1.5);
