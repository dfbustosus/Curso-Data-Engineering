import luigi
from luigi import Task 

class HolaMundo(Task):
    """
    Recordar que esto es lo que se necesita pero requires piede ser vacio si es la primera tarea
    1. requires
    2. output
    3. run
    """
    def output(self):
        return luigi.LocalTarget('archivo_1.txt')

    def run(self):
        print("hola soy David")
        with self.output().open('w') as f:
            f.write('Hola Mundo este es nuestro primer pipeline con Luigi')

if __name__ == '__main__':
    luigi.build([HolaMundo()], local_scheduler=True)

#if __name__ == '__main__':
#    luigi.run(['HolaMundo', '--local-scheduler'])