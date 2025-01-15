import pandas as pd


# Ruta de entrada y salida de los archivos
input_file = r"C:\Users\VOSTRO 3400\Documents\Portafolio de evidencias curso data engineer 2025\Practicas\ventas-x-ciudad.xlsx"
output_file = r"C:\Users\VOSTRO 3400\Documents\Portafolio de evidencias curso data engineer 2025\Practicas\ventas-x-ciudad.csv"

try:
    # Leer el archivo Excel
    data = pd.read_excel(input_file)
    
    # Guardar los datos en un archivo CSV
    data.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"Archivo convertido exitosamente a {output_file}")
except Exception as e:
    print(f"Error durante la conversión: {e}")


df = pd.read_csv(output_file)

print(df)

ventas_por_ciudad = df.groupby('Ciudad')['Ventas'].sum()

print(ventas_por_ciudad)