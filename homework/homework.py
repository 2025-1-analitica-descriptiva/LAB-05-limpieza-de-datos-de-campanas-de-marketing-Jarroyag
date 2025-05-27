"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import zipfile
import os
from glob import glob
from datetime import datetime

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months

    
    """
     # Crear la carpeta de salida si no existe
    output_dir = "files/output"
    os.makedirs(output_dir, exist_ok=True)

    # Ruta de los archivos ZIP de entrada
    input_files = sorted(glob("files/input/bank-marketing-campaing-*.csv.zip"))

    # Listas para almacenar los datos procesados
    client_data = []
    campaign_data = []
    economics_data = []

    # Procesar cada archivo ZIP
    for file in input_files:
        with zipfile.ZipFile(file, 'r') as zip_ref:
            for name in zip_ref.namelist():
                with zip_ref.open(name) as f:
                    # Leer el CSV comprimido en un DataFrame
                    df = pd.read_csv(f)

                    # Crear columna client_id única
                    df["client_id"] = df.index + sum(len(d) for d in client_data)

                    # -------------------- CLIENT --------------------
                    client_df = df[[
                        "client_id", "age", "job", "marital", "education",
                        "credit_default", "mortgage"
                    ]].copy()

                    # Limpiar columnas
                    client_df["job"] = client_df["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
                    client_df["education"] = client_df["education"].str.replace(".", "_", regex=False)
                    client_df["education"] = client_df["education"].replace("unknown", pd.NA)
                    client_df["credit_default"] = client_df["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
                    client_df["mortgage"] = client_df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)

                    client_data.append(client_df)

                   # -------------------- CAMPAIGN --------------------
                    campaign_df = df[[
                    "client_id", "number_contacts", "contact_duration",
                    "previous_campaign_contacts", "previous_outcome", 
                    "campaign_outcome", "day", "month"
                    ]].copy()

                    campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
                    campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)

                    # Crear fecha en formato YYYY-MM-DD con nombre correcto
                    campaign_df["last_contact_date"] = campaign_df.apply(
                    lambda row: datetime.strptime(f"{row['day']}-{row['month']}-2022", "%d-%b-%Y").strftime("%Y-%m-%d"),
                    axis=1
                    )
                    campaign_df.drop(columns=["day", "month"], inplace=True)
                    campaign_data.append(campaign_df)


                    # -------------------- ECONOMICS --------------------
                    economics_df = df[[
                        "client_id", "cons_price_idx", "euribor_three_months"
                    ]].copy()
                    economics_data.append(economics_df)

    # Guardar los resultados combinados como archivos CSV
    pd.concat(client_data).to_csv(os.path.join(output_dir, "client.csv"), index=False)
    pd.concat(campaign_data).to_csv(os.path.join(output_dir, "campaign.csv"), index=False)
    pd.concat(economics_data).to_csv(os.path.join(output_dir, "economics.csv"), index=False)

    return


if __name__ == "__main__":
    clean_campaign_data()