CREATE OR REPLACE PROCEDURE pETL_Insertar_DimTitulo()
AS $$
BEGIN
  DELETE FROM bde_dw.public.dimtitulos;
  INSERT INTO bde_dw.public.dimtitulos
    SELECT 
      t.titulo_id AS TituloId,
      t.titulo AS TituloNombre,
      CASE WHEN t.tipo = 'bbdd' THEN 'Base de datos, Transact-SQL'
           WHEN t.tipo = 'BI' THEN 'Base de datos, BI'
           WHEN t.tipo = 'administracion' THEN 'Base de datos, Administracion'
           WHEN t.tipo = 'dev' THEN 'Desarrollo'
           WHEN t.tipo = 'programacion' THEN 'Desarrollo'
      END AS TituloTipo,
      a.NombreAutor || ' ' || a.ApellidosAutor AS NombreCompleto,
      a.TelefonoAutor
    FROM bde.public.titulos AS t
    JOIN bde.public.autores AS a ON t.titulo_id = a.TituloId;
END; $$
LANGUAGE plpgsql;