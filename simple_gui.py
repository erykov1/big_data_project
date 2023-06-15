import tkinter as tk
from tkinter import messagebox
import commands


def quit_program(root):
    if messagebox.askokcancel("Potwierdzenie", "Czy na pewno chcesz wyłączyć program?"):
        root.destroy()


def show_plots():
    commands.show_charts()


def show_gui():
    root = tk.Tk()
    root.title("Prermier League")

    button_quit = tk.Button(root, text="Zamknij", command=lambda: quit_program(root))
    button_quit.pack(pady=10)

    button_show_plots = tk.Button(root, text="Pokaż wykresy", command=show_plots)
    button_show_plots.pack(pady=10)

    root.mainloop()
