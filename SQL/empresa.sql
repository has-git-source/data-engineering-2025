CREATE DATABASE empresa;
USE empresa;


CREATE TABLE clientes (
id_cliente INT AUTO_INCREMENT PRIMARY KEY,
nombre VARCHAR(50),
region VARCHAR(50)
);

INSERT INTO clientes (nombre, region) VALUES
('Jose Vargas', 'Norte'),
('Juan Ramirez', 'Sur'),
('Luisa Solis', 'Centro'),
('Fernanda Ruiz', 'Norte'),
('Sergio Martinez', 'Sur');




CREATE TABLE Pedido (
id_pedido INT AUTO_INCREMENT PRIMARY KEY,
precio DECIMAL(10,2),
Fecha DATE,
id_cliente INT,
FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente)
);



INSERT INTO Pedido (precio, fecha, id_cliente) VALUES
(10500.50, '2025-01-10', 1),
(6280.80, '2025-01-12', 3),
(5540.90, '2025-01-5', 4),
(7800.00, '2025-01-4', 5),
(4200.65, '2025-01-6', 2);



SELECT nombre, id_cliente  
FROM clientes   
WHERE id_cliente IN (SELECT id_cliente FROM Pedido   
WHERE fecha BETWEEN '2025-01-01' AND '2025-01-31');


SELECT 
    c.nombre, c.region, precio
FROM
    clientes AS c
        JOIN
    Pedido AS p ON c.id_cliente = p.id_cliente
WHERE
    p.precio > 6000;
    
SELECT c.nombre, c.region, MAX(p.precio) AS max_precio
FROM clientes AS c
JOIN Pedido AS p ON c.id_cliente = p.id_cliente
GROUP BY c.id_cliente, c.nombre, c.region
ORDER BY max_precio DESC;


SELECT c.nombre, c.region, MAX(p.precio) AS max_precio
FROM clientes AS c
JOIN Pedido AS p ON c.id_cliente = p.id_cliente
GROUP BY  c.nombre, c.region
ORDER BY max_precio DESC;


SELECT c.region, MAX(p.precio) AS max_precio
FROM clientes AS c
JOIN Pedido AS p ON c.id_cliente = p.id_cliente
GROUP BY c.region
ORDER BY max_precio DESC;