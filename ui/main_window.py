from PyQt5.QtWidgets import QMainWindow, QWidget, QHBoxLayout, QListWidget, QStackedWidget, QLabel

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PPS PRO Phase 2")
        self.resize(1100,650)

        central = QWidget()
        self.setCentralWidget(central)

        layout = QHBoxLayout(central)

        self.sidebar = QListWidget()
        self.sidebar.addItems(["Library","Chord","Karaoke","Settings"])
        layout.addWidget(self.sidebar,1)

        self.stack = QStackedWidget()
        self.stack.addWidget(QLabel("Library (Phase 2 Ready)"))
        self.stack.addWidget(QLabel("Chord Connected"))
        self.stack.addWidget(QLabel("Karaoke Connected"))
        self.stack.addWidget(QLabel("Settings (Theme + Language)"))

        layout.addWidget(self.stack,4)

        self.sidebar.currentRowChanged.connect(self.stack.setCurrentIndex)
