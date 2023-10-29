import luigi
import pandas as pd

class LeerCSV(luigi.Task):
    """
    Tarea para leer 'calls.csv' file.
    """
    # Archivo
    file_name = 'calls.csv'

    def output(self):
        return luigi.LocalTarget(self.file_name)

    def run(self):
        # Cargar el csv
        calls_data = pd.read_csv(self.file_name)
        # Guardar el DataFrame al output target
        calls_data.to_csv(self.output().path, index=False)

class Reporte(luigi.Task):
    """
    Obtener el reporte de llamadas
    1. requires
    2. output
    3. run
    """
    file_name = 'calls.csv'

    def requires(self):
        return LeerCSV()

    def output(self):
        return luigi.LocalTarget('call_counts.json')

    def run(self):
        # Cargar el output de ReadCallsCSV
        calls_data = pd.read_csv(self.input().path)

        # Calcular los conteos
        product_sold_count = calls_data[calls_data['productsold'] == 1].shape[0]
        picked_up_count = calls_data[calls_data['pickedup'] == 1].shape[0]
        total_calls = len(calls_data)

        # Crear un dict sencillo
        counts = {
            'productSold_calls': product_sold_count,
            'pickedup_calls': picked_up_count,
            'total_calls': total_calls
        }

        # Guardar el JSON
        with self.output().open('w') as f:
            f.write(pd.Series(counts).to_json(indent=4))

if __name__ == '__main__':
    luigi.build([Reporte()], local_scheduler=True)