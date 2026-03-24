def apply_dark_blue_theme(app):
    app.setStyleSheet("""
    QMainWindow {
        background-color: #0b1220;
    }

    QWidget {
        color: #e5e7eb;
        font-family: Segoe UI;
        font-size: 13px;
    }

    QPushButton {
        background-color: #16223a;
        border: 1px solid #22304d;
        border-radius: 8px;
        padding: 6px 12px;
    }

    QPushButton:hover {
        background-color: #1c2a45;
    }

    QPushButton:pressed {
        background-color: #3b82f6;
    }

    QFrame {
        background-color: #111a2e;
        border-radius: 10px;
    }

    QTableWidget {
        background-color: #111a2e;
        gridline-color: #22304d;
    }

    QHeaderView::section {
        background-color: #16223a;
        border: none;
        padding: 5px;
    }

    QScrollBar:vertical {
        background: #0b1220;
        width: 8px;
    }

    QScrollBar::handle:vertical {
        background: #22304d;
        border-radius: 4px;
    }

    QScrollBar::handle:vertical:hover {
        background: #3b82f6;
    }
    """)
