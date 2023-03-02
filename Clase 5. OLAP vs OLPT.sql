/* 
Imagen 1:
Imaginemos una escuela que tiene distintos roles (Estudiante, Principal, Parientes, Profesores)
Guardaremos la info en una DB estructurada
*/

/*
Imagen 2
Que es una DB estructurada, bueno piensa en una base de dtaos de telefonos
Bueno en esta base de datos podemos insertar, borrar, modificar o cambiar 
algunas propiedades de cualquier dato
*/

/*
Imagen 3
Ahora imagina que la informacion de la escuela va creciendo y almacenamos datos
por 5 años

Si un estudiante se graduo hace mucho tiempo, deberiamos mantener su informacion?
Y donde deberiamos almacenar la info?
DW = sistemas que permiten almacenar datos agregados que tienen un nivel mas alto
que se encuentran en la base de datos 
*/

/*Imagen 4
Podrias uar la info de los estudiantes de hace mas de 5 años para propositos como
- Cuantos estudiantes se han graduado?
- Saber que genero tiene mas graduados?
- Identificar cuando duran en tu escuela los estudiantes?
- Conocer el perfil de egreso de tus estudiantes?
*/

/* Imagen 5
Ahora piensa David es un estudiante de la escuela y sus padres van a preguntar sobre 
como le esta llendo, supongamos que tienen dos preguntas?
1. Como le esta llendo a David en clase?
2. Mi hijo le ira bien en esta escuala?

Ahora supongamos que los padres hacen la primera pregunta al maestro
Probablemente reciban dos respuestas:
a) Le esta llendo muy bien
b) Le esta llendo muy mal y no estudia

Si le hacen la misma pregunta el director probablemente diga ¿Quien es David?
Bien piensa al maestro como una DB y al director como un DW
El maestro puede ver a David todos los dias lo cual les permite tener info actualizada de David
El director solo podria encontrar esta info si se le diera su clase pero le costaria mucho

Ahora piensa en la segunda pregunta probablemente la maestra no podria responder, le tocaria
buscar y preguntar a diferentes profesores en distintas materias para saber
El director probablemente si podra responder esto facilmente (hace una agregacion de datos o un 
summary a traves del tiempo)

Con esto ya deberas tener claro la diferencia entre OLAP y OLTP

OLTP : Online transaction Processing
Sistema de informacion que facilita y maneja las transacciones
orientadas a aplicaciones

OLAP: Online analytical processing
Aproximaciones para responder queries analiticas multi-dimensionales
de forma eficiente
*/

/*
Imagen 6: Normalizacion vs Denormalizacion
Normalizacion: garantizar data consistente sin redundancia manteniendo entidades
separadas para evitar repeticiones (OLTP)
Denormalizacion: Seria tener todo en modo de summary (e.g tener las calificaciones
de los estudiantes )-- OLAP
*/


-- Parte II (Practica)

-- Momento 0. Crear la DB
CREATE DATABASE "Escuelas"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- Momento 1 (creacion de tablas)
CREATE TABLE department (
  department_id INT PRIMARY KEY,
  name VARCHAR(50)
);

CREATE TABLE teacher (
  teacher_id INT PRIMARY KEY,
  name VARCHAR(50),
  number_of_complaints INT,
  department_id INT,
  FOREIGN KEY (department_id) REFERENCES department(department_id)
);

CREATE TABLE class (
  class_id INT PRIMARY KEY,
  name VARCHAR(50),
  teacher_id INT,
  FOREIGN KEY (teacher_id) REFERENCES teacher(teacher_id)
);

CREATE TABLE student (
  student_id INT PRIMARY KEY,
  name VARCHAR(50),
  age INT,
  class_id INT,
  FOREIGN KEY (class_id) REFERENCES class(class_id)
);

-- Momento 2 (insercion de registros)
INSERT INTO department (department_id, name)
VALUES (1, 'Mathematics'),
       (2, 'Science'),
       (3, 'English'),
       (4, 'Biology');

INSERT INTO teacher (teacher_id, name, number_of_complaints, department_id)
VALUES (1, 'John Smith', 20, 1),
       (2, 'Samantha Brown', 10, 2),
       (3, 'Mike Johnson', 30, 3),
       (4, 'Pedro Arlow', 30, 4),
       (5, 'Juan Parlow', 10, 1),
       (6, 'Andres Button', 50, 2),
       (7, 'Andrea Buttingam', 20, 3),
       (8, 'Sofia Andrt', 10, 4);

INSERT INTO class (class_id, name, teacher_id)
VALUES (1, 'Algebra 1', 1),
       (2, 'Algebra 2', 5),
       (3, 'Biology', 4),
       (4, 'Biology', 8),
       (5, 'English Literature', 3),
       (6, 'English Advanced', 7),
       (7, 'Science I', 2),
       (8, 'Science II', 6);

INSERT INTO student (student_id, name, age, class_id)
VALUES (1, 'Bob Johnson', 16, 1),
       (2, 'Bob aty', 16, 1),
       (3, 'Sarah Lee', 15, 2),
       (4, 'Carolina Path', 14, 2),
       (5, 'Tom Smith', 17, 3),
       (6, 'Andrew Sanz', 12, 3),
       (7, 'Sofia Cart', 13, 4),
       (8, 'Luigi Pard', 15, 4);

-- Debemos remover a los profesores con mas quejas de sus cargos
-- Momento 3. OLTP 
SELECT name 
FROM teacher
WHERE number_of_complaints >25

-- Ahora necesitamos saber en que departamento invertir ?
-- Momento 4. OLAP
/*
Esta consulta devolverá el número total de quejas de cada departamento, ordenadas de forma descendente. 
Con esto podemos determinar qué departamento puede necesitar inversiones o recursos adicionales.
*/
SELECT department.name, SUM(teacher.number_of_complaints) as total_complaints
FROM department
JOIN teacher ON department.department_id = teacher.department_id
GROUP BY department.name
ORDER BY total_complaints DESC;
