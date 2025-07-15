import os
import argparse
import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox


DEFAULT_PADRON = "Padron_Lixo.xlsx"


def detect_columns(df: pd.DataFrame, filename: str):
    lower = filename.lower()
    if "dni_y_direccion" in lower:
        return "refs_candidatas_dni_dir", "num_candidatos_dni_dir", "coincidencia dni y dirección"
    if "dni" in lower and "direccion" not in lower:
        return "refs_candidatas_dni", "num_candidatos_dni", "coincidencia dni"
    if "direccion" in lower:
        return "refs_candidatas_dir", "num_candidatos_dir", "coincidencia dirección"

    ref_col = next((c for c in df.columns if c.startswith("refs_candidatas")), None)
    num_col = None
    if ref_col:
        base = ref_col.replace("refs_candidatas", "num_candidatos")
        if base in df.columns:
            num_col = base
    return ref_col, num_col, ""


class AssignApp:
    def __init__(self, master, padron_df, cand_df, ref_col, num_col, reason_default):
        self.master = master
        self.padron = padron_df
        self.candidates = cand_df
        self.ref_col = ref_col
        self.num_col = num_col
        self.reason_default = reason_default
        self.index = 0

        if "Ref_Catastral_Asignada" not in self.padron.columns:
            self.padron["Ref_Catastral_Asignada"] = pd.NA
        if "Motivo_Asignacion" not in self.padron.columns:
            self.padron["Motivo_Asignacion"] = pd.NA

        self.id_label = tk.Label(master, text="")
        self.id_label.pack(pady=5)

        self.combo = ttk.Combobox(master)
        self.combo.pack(pady=5)

        self.reason_entry = tk.Entry(master, width=50)
        self.reason_entry.pack(pady=5)

        btn_frame = tk.Frame(master)
        btn_frame.pack(pady=5)
        tk.Button(btn_frame, text="Asignar", command=self.assign).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Saltar", command=self.skip).pack(side=tk.LEFT, padx=5)
        tk.Button(btn_frame, text="Guardar", command=self.save).pack(side=tk.LEFT, padx=5)

        self.load_row()

    def load_row(self):
        if self.index >= len(self.candidates):
            messagebox.showinfo("Fin", "No quedan filas por procesar")
            return
        row = self.candidates.iloc[self.index]
        ident = row.get("identificador", "")
        refs = row.get(self.ref_col, "")
        self.current_id = ident
        self.id_label.config(text=f"Identificador: {ident}")

        ref_list = [r.strip() for r in str(refs).split(",") if r.strip()]
        self.combo['values'] = ref_list
        if len(ref_list) == 1:
            self.combo.current(0)
            self.reason_entry.delete(0, tk.END)
            self.reason_entry.insert(0, self.reason_default)
        else:
            self.combo.set('')
            self.reason_entry.delete(0, tk.END)

    def assign(self):
        if self.index >= len(self.candidates):
            return
        ref = self.combo.get().strip()
        if not ref:
            messagebox.showwarning("Advertencia", "Seleccione una referencia")
            return
        reason = self.reason_entry.get().strip()
        mask = self.padron.get("identificador") == self.current_id
        if mask.any():
            self.padron.loc[mask, "Ref_Catastral_Asignada"] = ref
            self.padron.loc[mask, "Motivo_Asignacion"] = reason
        self.index += 1
        self.load_row()

    def skip(self):
        self.index += 1
        self.load_row()

    def save(self):
        self.padron.to_excel(DEFAULT_PADRON, index=False)
        messagebox.showinfo("Guardado", f"Datos guardados en {DEFAULT_PADRON}")


def main(padron_path, candidatos_path):
    padron = pd.read_excel(padron_path, dtype=str)
    cand = pd.read_excel(candidatos_path, dtype=str)

    ref_col, num_col, reason_default = detect_columns(cand, candidatos_path)
    if not ref_col or ref_col not in cand.columns:
        raise ValueError("No se pudo detectar la columna de referencias candidatas")

    root = tk.Tk()
    root.title("Asignar referencias")
    app = AssignApp(root, padron, cand, ref_col, num_col, reason_default)
    root.mainloop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Asignar referencias candidatas")
    parser.add_argument("candidatos", help="Archivo de candidatos")
    parser.add_argument("--padron", default=DEFAULT_PADRON, help="Archivo Padron_Lixo")
    args = parser.parse_args()
    main(args.padron, args.candidatos)

