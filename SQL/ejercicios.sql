CREATE DATABASE practicas;
USE practicas;
CREATE TABLE empleados (
    empleado_id INT AUTO_INCREMENT PRIMARY KEY, 
    nombre VARCHAR(100) NOT NULL,    
    edad INT,
    puesto VARCHAR(50) NOT NULL,              
    salario DECIMAL(10, 2) NOT NULL            
);

INSERT INTO empleados (nombre, edad, puesto, salario) VALUES
('Juan Pérez', 26 , 'Gerente', 50000.00),
('María López', 24, 'Asistente', 25000.00),
('Carlos García', 30,'Analista', 30000.00),
('Ana Torres', 29,'Desarrollador', 45000.00),
('Sofía Ortiz', 31, 'Diseñadora', 40000.00),
('Luis Fernández', 45, 'Administrador', 55000.00),
('Elena Cruz', 33, 'Marketing', 32000.00),
('Diego Romero', 26, 'Ventas', 28000.00);


SELECT empleado_id, nombre, edad
FROM empleados;

SELECT * 
FROM empleados 
WHERE edad > 30;


CREATE TABLE empleados2 (
    empleado_id INT AUTO_INCREMENT PRIMARY KEY, 
    nombre VARCHAR(100) NOT NULL,                
    edad INT NOT NULL,                           
    ventas DECIMAL(10,2) NOT NULL DEFAULT 0.00,  
    descripcion VARCHAR(255) NOT NULL DEFAULT '',
    fecha DATE NOT NULL                          
);



INSERT INTO empleados2 (nombre, edad, ventas, descripcion, fecha) VALUES
('Juan Pérez', 40, 15000.00, 'productos tecnológicos', '2025-01-10'),
('María López', 35, 20000.00, 'soluciones', '2025-01-12'),
('Carlos García', 32, 12000.00, 'software', '2025-01-15'),
('Ana Torres', 29, 18000.00, 'desarrollo web', '2025-01-18'),
('Laura Méndez', 30, 25000.00, 'servicios financieros', '2025-01-2'),
('Pedro Sánchez', 25, 0.00, 'ventas este dia', '2025-01-3'),
('Sofía Ortiz', 28, 22000.00, 'marketing digital', '2025-01-4'),
('Luis Fernández', 45, 19000.00, 'consultoría empresarial', '2025-01-9'),
('Elena Cruz', 27, 21000.00, 'productos de consumo', '2025-01-3'),
('Diego Romero', 22, 0.00, 'No hubo ventas este dia', '2025-01-10');




SELECT nombre, ventas, descripcion, fecha
FROM empleados2
WHERE MONTH(fecha) = 1;  


SELECT nombre, ventas, descripcion, fecha
FROM empleados2
WHERE ventas = 0.00;

CREATE DATABASE practicajoin;
USE practicajoin;


CREATE TABLE izquierda (
izquierda1 VARCHAR(60) NOT NULL,
izquierda2 VARCHAR(60) NOT NULL,
izquierda3 VARCHAR(60) NOT NULL
);

CREATE TABLE derecha (
derecha1 VARCHAR(60) NOT NULL,
derecha2 VARCHAR(60) NOT NULL,
dercha3 VARCHAR(60)NOT NULL
);

CREATE TABLE frente (
fente1 VARCHAR(60) NOT NULL,
frente2 VARCHAR(60) NOT NULL,
frente3 VARCHAR(60) NOT NULL
);


INSERT INTO izquierda (izquierda1, izquierda2, izquierda3) VALUES
("uno", "dos", "tres");

INSERT INTO derecha (derecha1, derecha2, dercha3) VALUES
("cuatro", "cinco", "seis");

INSERT INTO frente (fente1, frente2, frente3) VALUES
("siete", "ocho", "nueve");

INSERT INTO derecha (derecha1, derecha2, dercha3) VALUES
("uno", "dos", "tres");

INSERT INTO frente (fente1, frente2, frente3) VALUES
("uno", "dos", "tres");

SELECT izquierda1 
FROM izquierda as i
JOIN derecha as d
ON i.izquierda1 = d.derecha1;


SELECT 
    izquierda1
FROM
    izquierda AS i
        RIGHT JOIN
    derecha AS d ON i.izquierda1 = d.derecha1;



SELECT frente

