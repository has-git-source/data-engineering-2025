CREATE DATABASE tienda;
USE tienda;

CREATE TABLE Productos (
	id_producto INT AUTO_INCREMENT PRIMARY KEY, 
	nombre VARCHAR(50),
	precio DECIMAL(10,2),
	categoria VARCHAR(50)
);


CREATE TABLE Ventas (
    id_venta INT AUTO_INCREMENT PRIMARY KEY, 
    vendedor VARCHAR(50),
    producto VARCHAR(50),
	id_producto INT,
    FOREIGN KEY (id_producto) REFERENCES Productos(id_producto)
);

INSERT INTO Ventas (vendedor, producto, id_producto) VALUES
('Jose Vargas', 'Laptop', 1),
('Juan Ramirez', 'Audifonos',2 ),
('Luisa Solis', 'Memoria 50GB', 6),
('Fernanda Ruiz', 'Mochila', 7),
('Sergio Martinez', 'Audifonos',2),
('Jair Sanchez', 'Camara', 7),
('Jorge Hernandez', 'Silla', 7);


INSERT INTO Productos (nombre, precio, categoria) VALUES
('Laptop', 10550.00, 'Computo'),
('Audifonos', 550.50, 'Accesorios'),
('Memoria 1TB', 2100.00, 'Almacenamiento'),
('Mochila', 300.00, 'Accesorios'),
('Camara HD', 1000.99, 'Camaras');

INSERT INTO Productos (nombre, precio, categoria) VALUES
('Memoria 50GB', 1800.00, 'Almacenamiento'),
('Producto genérico', 100.00, 'Otros');


SELECT p.nombre, p.precio, v.vendedor, v.id_producto
FROM Productos AS p
INNER JOIN Ventas AS v
ON p.id_producto = v.id_producto;


SELECT v.producto, v.id_producto
FROM Ventas AS v
RIGHT JOIN Productos AS P
ON  v.id_producto = p.id_producto;