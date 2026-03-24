from PyQt5.QtWidgets import QApplication
import sys
from ui.main_window import MainWindow

app = QApplication(sys.argv)
w = MainWindow()
w.show()
sys.exit(app.exec_())
