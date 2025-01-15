import pandas as pd 
data = {
  'Empleado': ['Carlos', 'Marta', 'Luis'],
  'Salario': [2500, 3000, 2800]
}
empleados_df = pd.DataFrame(data)
media_salario = empleados_df['Salario'].mean()
print(f"Salario medio: {media_salario}")

filtro = empleados_df[empleados_df['Salario'] > 2700] 

print(filtro) 