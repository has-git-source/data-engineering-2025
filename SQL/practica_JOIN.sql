CREATE DATABASE PracticarJoin;
USE PracticarJoin;

CREATE TABLE Empleados (
    id_empleado INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(50),
    salario DECIMAL(10, 2),
    id_departamento INT
);


CREATE TABLE Departamentos (
    id_departamento INT AUTO_INCREMENT PRIMARY KEY,
    nombre_departamento VARCHAR(50)
);


INSERT INTO Departamentos (nombre_departamento)
VALUES 
    ('Recursos Humanos'),
    ('Desarrollo'),
    ('Ventas'),
    ('Marketing');
    
INSERT INTO Empleados (nombre, salario, id_departamento)
VALUES
    ('Juan Pérez', 25000.00, 1),
    ('María López', 30000.00, 2),
    ('Luis Gómez', 28000.00, 3),
    ('Ana García', 27000.00, NULL),
    ('Carlos Torres', 35000.00, 2);
    

SELECT 
    Empleados.nombre AS Nombre_Empleado,
    Departamentos.nombre_departamento AS Departamento,
    Empleados.salario AS Salario
FROM 
    Empleados
INNER JOIN 
    Departamentos
ON 
    Empleados.id_departamento = Departamentos.id_departamento;
    
    
SELECT 
    Empleados.nombre AS Nombre_Empleado,
    Departamentos.nombre_departamento AS Departamento,
    Empleados.salario AS Salario
FROM 
    Empleados
LEFT JOIN 
    Departamentos
ON 
    Empleados.id_departamento = Departamentos.id_departamento;

SELECT 
    Empleados.nombre AS Nombre_Empleado,
    Departamentos.nombre_departamento AS Departamento,
    Empleados.salario AS Salario
FROM 
    Empleados
RIGHT JOIN 
    Departamentos
ON 
    Empleados.id_departamento = Departamentos.id_departamento;
    
    
INSERT INTO Empleados (nombre, salario, id_departamento)
VALUES
    ('Mario Perez', 25000.00, NULL),
    ('Angelica López', 30000.00, 6),
    ('Roberto Gómez', 28000.00, 3),
    ('Rodrigo García', 27000.00, NULL),
    ('Karla Ruiz', 35000.00, 5);

INSERT INTO Departamentos (nombre_departamento)
VALUES 
    ('Reporting'),
    ('Legal'),
    ('Consultoria');
    
    
ALTER TABLE Empleados
ADD edad INT;


UPDATE Empleados
SET edad = 30
WHERE id_empleado = 1;

UPDATE Empleados
SET edad = 25
WHERE id_empleado = 2;

UPDATE Empleados
SET edad = 35
WHERE id_empleado = 3;

UPDATE Empleados
SET edad = 40
WHERE id_empleado = 4;

UPDATE Empleados
SET edad = 29
WHERE id_empleado = 5;

UPDATE Empleados
SET edad = 33
WHERE id_empleado = 6;

UPDATE Empleados
SET edad = 27
WHERE id_empleado = 7;

UPDATE Empleados
SET edad = 31
WHERE id_empleado = 8;

UPDATE Empleados
SET edad = 26
WHERE id_empleado = 9;

UPDATE Empleados
SET edad = 38
WHERE id_empleado = 10;


SELECT nombre, edad FROM Empleados; 

SELECT nombre, salario 
FROM Empleados 
WHERE salario > 30000;

SELECT nombre_departamento
FROM Departamentos; 

SELECT 
    nombre
FROM
    Empleados
WHERE
    id_departamento = (SELECT 
            id_departamento
        FROM
            Departamentos
        WHERE
            nombre_departamento = 'Ventas'); 
            
            
SELECT Empleados.nombre, Empleados.salario, Departamentos.nombre_departamento AS departamento 

FROM Empleados 

INNER JOIN Departamentos 

ON Empleados.id_departamento = Departamentos.id_departamento;


SELECT nombre, salario   

FROM Empleados   

WHERE salario > (SELECT AVG(salario) FROM Empleados);


CREATE TABLE Ventas (
ventas INT,
region VARCHAR(50)
);


INSERT INTO Ventas (ventas, region) VALUES
(50, 'Monterrey'),
(60, 'Guadalajara'),
(100, 'CDMX'),
(20, 'Monterrey'),
(30, 'CDMX'),
(20, 'Guadalajara'),
(30, 'Monterrey'),
(10, 'CDMX'),
(5, 'Guadalajara');

SELECT region, SUM(ventas) AS total_ventas   
FROM Ventas   
GROUP BY region;
