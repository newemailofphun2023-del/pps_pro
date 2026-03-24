from PyQt5.QtWidgets import QApplication
import sys

from ui.main_window import MainWindow
from theme import apply_dark_blue_theme   # ⭐ เพิ่ม

app = QApplication(sys.argv)

apply_dark_blue_theme(app)   # ⭐ ใช้ theme กลาง

w = MainWindow()
w.show()

sys.exit(app.exec_())
