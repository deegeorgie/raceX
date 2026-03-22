"""Interactive Matplotlib heatmap with controls for Qt."""

# PyQt5 is optional; Streamlit imports this module for shared logic.
try:
    from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, QComboBox,
                                 QListWidget, QListWidgetItem, QDoubleSpinBox, QPushButton,
                                 QFileDialog, QMessageBox, QCheckBox, QScrollArea)
    from PyQt5.QtCore import Qt, QSize
    from PyQt5.QtGui import QColor
    PYQT_AVAILABLE = True
    _PYQT_IMPORT_ERROR = None
except Exception as _e:
    PYQT_AVAILABLE = False
    _PYQT_IMPORT_ERROR = _e

    def _pyqt_unavailable(*_args, **_kwargs):
        raise ImportError("PyQt5 is required for the interactive heatmap GUI but is not installed.")

    class _PyQtBase:
        def __init__(self, *_args, **_kwargs):
            _pyqt_unavailable()

    QWidget = QVBoxLayout = QHBoxLayout = QLabel = QComboBox = _PyQtBase
    QListWidget = QListWidgetItem = QDoubleSpinBox = QPushButton = _PyQtBase
    QFileDialog = QMessageBox = QCheckBox = QScrollArea = _PyQtBase
    Qt = QSize = QColor = _PyQtBase
import pandas as pd
import matplotlib
# Ensure we use the Qt5Agg backend for interactive Qt embedding when available
if PYQT_AVAILABLE:
    try:
        matplotlib.use('Qt5Agg')
    except Exception:
        pass
import matplotlib.pyplot as plt
if PYQT_AVAILABLE:
    try:
        from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
    except Exception:
        FigureCanvas = None
else:
    FigureCanvas = None
import textwrap

# Compact control stylesheet for heatmap controls
CONTROL_STYLE = '''
QWidget { background: transparent; }
QComboBox, QListWidget, QDoubleSpinBox, QPushButton {
    background-color: #2b2b2b;
    color: #f0f0f0;
    border: 1px solid #444444;
    padding: 4px 6px;
    border-radius: 6px;
    font-size: 11px;
}
QComboBox::drop-down { subcontrol-origin: padding; subcontrol-position: top right; width: 18px; }
QComboBox::down-arrow { image: none; }
QListWidget::item { padding: 6px 8px; }
QListWidget { background-color: rgba(43,43,43,0.9); }
QPushButton#exportBtn { background-color: #27ae60; color: white; font-weight: bold; }
QPushButton#exportBtn:hover { background-color: #2ecc71; }
QPushButton { border: 1px solid #3a3a3a; }
QDoubleSpinBox { min-width: 72px; }
'''


def create_interactive_matplotlib_heatmap(norm_df, composite_scores=None, parent=None, external_controls=None, on_metric_toggle=None):
    """Create an interactive heatmap with controls using Matplotlib.
    Supports metric selection, sorting, vmin/vmax, and colormap changes.
    
    Args:
        on_metric_toggle: Optional callback function(metric_name, is_checked) to trigger
                         when a metric checkbox is toggled (for external recomputation).
    """
    if not PYQT_AVAILABLE or FigureCanvas is None:
        raise ImportError("PyQt5 is required for the interactive heatmap GUI but is not installed.")
    if norm_df.empty:
        print("[WARN] norm_df is empty, returning empty widget")
        return QWidget(parent)

    container = QWidget(parent)
    layout = QVBoxLayout(container)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(4)
    
    # Controls area. If external_controls dict is provided, use those widgets
    controls = QWidget()
    ctrl_layout = QHBoxLayout(controls)
    ctrl_layout.setContentsMargins(4, 4, 4, 4)

    # Metric selector (horizontal checkboxes inside a scrollable area)
    metrics_container = QWidget()
    metrics_hlayout = QHBoxLayout(metrics_container)
    metrics_hlayout.setContentsMargins(0, 0, 0, 0)
    metrics_hlayout.setSpacing(8)

    metrics_checkboxes = []
    for col in norm_df.columns:
        try:
            cb = QCheckBox(col)
            cb.setChecked(True)
            cb.setStyleSheet('padding:4px;')
            metrics_hlayout.addWidget(cb)
            metrics_checkboxes.append(cb)
        except Exception:
            pass

    metrics_hlayout.addStretch()

    scroll = QScrollArea()
    scroll.setWidgetResizable(True)
    scroll.setMaximumHeight(60)
    scroll.setWidget(metrics_container)

    # Add select/clear buttons to make checking easier
    btns_widget = QWidget()
    btns_layout = QVBoxLayout(btns_widget)
    btns_layout.setContentsMargins(0, 0, 0, 0)
    select_all_btn = QPushButton('Tout sélectionner')
    clear_all_btn = QPushButton('Déselectionner')
    select_all_btn.setMaximumWidth(120)
    clear_all_btn.setMaximumWidth(120)
    btns_layout.addWidget(select_all_btn)
    btns_layout.addWidget(clear_all_btn)
    btns_layout.addStretch()

    ctrl_layout.addWidget(QLabel("Métriques:"))
    ctrl_layout.addWidget(scroll, 1)
    ctrl_layout.addWidget(btns_widget)

    try:
        metrics_container.setStyleSheet(CONTROL_STYLE)
    except Exception:
        pass

    # Select / Clear handlers for new checkbox list
    def _select_all():
        try:
            for cb in metrics_checkboxes:
                cb.setChecked(True)
            render_plot()
        except Exception:
            pass

    def _clear_all():
        try:
            for cb in metrics_checkboxes:
                cb.setChecked(False)
            render_plot()
        except Exception:
            pass

    try:
        select_all_btn.clicked.connect(_select_all)
        clear_all_btn.clicked.connect(_clear_all)
    except Exception:
        pass

    # External controls: expect a dict with keys 'sort_combo','vmin_spin','vmax_spin','cmap_combo','export_btn'
    if external_controls and isinstance(external_controls, dict):
        sort_combo = external_controls.get('sort_combo')
        vmin_spin = external_controls.get('vmin_spin')
        vmax_spin = external_controls.get('vmax_spin')
        cmap_combo = external_controls.get('cmap_combo')
        export_btn = external_controls.get('export_btn')
        # do not add them to the canvas controls UI (they live externally)
        try:
            # apply stylesheet to external widgets if present
            if isinstance(sort_combo, QComboBox):
                sort_combo.setStyleSheet(CONTROL_STYLE)
            if isinstance(cmap_combo, QComboBox):
                cmap_combo.setStyleSheet(CONTROL_STYLE)
            if isinstance(vmin_spin, QDoubleSpinBox):
                vmin_spin.setStyleSheet(CONTROL_STYLE)
            if isinstance(vmax_spin, QDoubleSpinBox):
                vmax_spin.setStyleSheet(CONTROL_STYLE)
            if isinstance(export_btn, QPushButton):
                export_btn.setObjectName('exportBtn')
                export_btn.setStyleSheet(CONTROL_STYLE)
        except Exception:
            pass
        layout.addWidget(controls)
    else:
        # Sort dropdown
        sort_combo = QComboBox()
        sort_combo.addItem('Composite')
        for col in norm_df.columns:
            sort_combo.addItem(col)
        sort_combo.setMaximumWidth(180)
        ctrl_layout.addWidget(QLabel('Trier par:'))
        ctrl_layout.addWidget(sort_combo)

        # vmin/vmax
        vmin_spin = QDoubleSpinBox()
        vmax_spin = QDoubleSpinBox()
        vmin_spin.setRange(0.0, 1.0)
        vmax_spin.setRange(0.0, 1.0)
        vmin_spin.setSingleStep(0.01)
        vmax_spin.setValue(1.0)
        vmin_spin.setMaximumWidth(80)
        vmax_spin.setMaximumWidth(80)
        ctrl_layout.addWidget(QLabel('vmin'))
        ctrl_layout.addWidget(vmin_spin)
        ctrl_layout.addWidget(QLabel('vmax'))
        ctrl_layout.addWidget(vmax_spin)

        # colormap selector
        cmap_combo = QComboBox()
        cmap_combo.addItems(['RdYlGn', 'Viridis', 'Plasma', 'Cividis'])
        cmap_combo.setMaximumWidth(140)
        ctrl_layout.addWidget(QLabel('Colormap'))
        ctrl_layout.addWidget(cmap_combo)

        # Export button
        export_btn = QPushButton("Exporter PNG")
        export_btn.setMaximumWidth(100)
        export_btn.setObjectName('exportBtn')
        try:
            export_btn.setStyleSheet(CONTROL_STYLE)
        except Exception:
            pass
        ctrl_layout.addWidget(export_btn)

        try:
            controls.setStyleSheet(CONTROL_STYLE)
        except Exception:
            pass

        layout.addWidget(controls)

    # Canvas container for matplotlib
    canvas_container = QWidget()
    canvas_layout = QVBoxLayout(canvas_container)
    canvas_layout.setContentsMargins(0, 0, 0, 0)
    canvas_layout.setSpacing(0)
    layout.addWidget(canvas_container, 1)

    print("[DEBUG] Controls created, setting up matplotlib renderer")

    # Store last figure for export
    last_fig = None

    # renderer function using matplotlib
    def render_plot():
        nonlocal last_fig
        try:
            print("[DEBUG] render_plot called")
            # selected metrics (support both new checkbox layout and legacy list)
            try:
                selected = [cb.text() for cb in metrics_checkboxes if cb.isChecked()]
            except Exception:
                try:
                    selected = [metrics_list.item(i).text() for i in range(metrics_list.count()) 
                               if metrics_list.item(i).checkState() == Qt.Checked]
                except Exception:
                    selected = list(norm_df.columns)
            if not selected:
                selected = list(norm_df.columns)

            # build display df
            disp = norm_df[selected].copy()

            # determine sorting
            sort_key = sort_combo.currentText()
            if sort_key == 'Composite' and composite_scores is not None:
                order = [h for h in composite_scores.index if h in disp.index]
                disp = disp.reindex(order)
            elif sort_key in disp.columns:
                disp = disp.sort_values(by=sort_key, ascending=False)
            else:
                # If sort_key not in selected columns, calculate mean of selected metrics for sorting
                # This ensures horses are re-ranked based on currently selected metrics
                try:
                    selected_metrics_mean = disp.mean(axis=1)  # Average across selected metrics for each horse
                    disp = disp.iloc[selected_metrics_mean.argsort()[::-1]]  # Sort by mean (descending)
                    print(f"[DEBUG] Re-sorted by mean of selected metrics: {selected}")
                except Exception as e:
                    print(f"[DEBUG] Could not sort by selected metrics mean: {e}")

            # get color scale settings
            colormap = cmap_combo.currentText()
            vmin = float(vmin_spin.value())
            vmax = float(vmax_spin.value())

            # Create matplotlib figure with dynamic sizing and constrained layout
            num_horses = len(disp.index)
            n_cols = len(disp.columns)
            # width scales with number of metrics, height with number of horses
            fig_width = max(8, min(24, 0.6 * n_cols))
            fig_height = max(3, min(40, 0.35 * num_horses))
            fig, ax = plt.subplots(figsize=(fig_width, fig_height), constrained_layout=True)

            # Create heatmap
            im = ax.imshow(disp.values, aspect='auto', cmap=colormap, vmin=vmin, vmax=vmax)

            # Configure axes
            # Wrap long x-axis labels to multiple lines to avoid truncation
            wrapped_xticks = [textwrap.fill(str(lbl), width=18) for lbl in disp.columns]
            ax.set_xticks(range(len(disp.columns)))
            ax.set_xticklabels(wrapped_xticks, rotation=45, ha='right', fontsize=10, fontweight='bold')
            # Wrap y-axis horse names if very long; reduce font size for many horses
            y_fontsize = 9 if num_horses <= 30 else max(6, int(180/num_horses))
            wrapped_yticks = [textwrap.fill(str(lbl), width=22) for lbl in disp.index]
            ax.set_yticks(range(len(disp.index)))
            ax.set_yticklabels(wrapped_yticks, fontsize=y_fontsize, fontweight='bold')
            ax.tick_params(axis='x', labelsize=10, pad=6)
            ax.tick_params(axis='y', labelsize=y_fontsize)

            # Add gridlines
            ax.set_xticks([x - 0.5 for x in range(1, len(disp.columns))], minor=True)
            ax.set_yticks([y - 0.5 for y in range(1, len(disp.index))], minor=True)
            ax.grid(which='minor', color='white', linestyle='-', linewidth=1.5)

            # Add text annotations
            for i in range(len(disp.index)):
                for j in range(len(disp.columns)):
                    val = disp.values[i, j]
                    text_color = 'white' if val < 0.5 else 'black'
                    ax.text(j, i, f'{val:.2f}', ha='center', va='center', 
                           color=text_color, fontsize=8, fontweight='bold')

            # Colorbar
            cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
            cbar.set_label('Strength (0=Low, 1=High)', fontsize=10, fontweight='bold')

            # Title
            ax.set_title(f'Horse Performance Heatmap (sorted by {sort_key})', 
                       fontsize=12, fontweight='bold', pad=15)
            
            # tight layout already handled by constrained_layout, but ensure padding
            try:
                fig.tight_layout(pad=1.0)
            except Exception:
                pass

            # Clear previous canvas and add new one (use takeAt to fully remove)
            for i in reversed(range(canvas_layout.count())):
                item = canvas_layout.takeAt(i)
                if item is None:
                    continue
                w = item.widget()
                if w:
                    try:
                        w.setParent(None)
                    except Exception:
                        pass
                    try:
                        w.deleteLater()
                    except Exception:
                        pass

            canvas = FigureCanvas(fig)
            canvas_layout.addWidget(canvas)
            # Improve rendering reliability: set size policy and minimum size
            try:
                from PyQt5.QtWidgets import QSizePolicy
                canvas.setMinimumSize(400, 240)
                canvas.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            except Exception:
                pass
            try:
                canvas.draw()
            except Exception:
                try:
                    canvas.draw_idle()
                except Exception:
                    pass
            # keep a reference for export and prevent garbage collection by storing on container
            last_fig = fig
            try:
                container._heatmap_canvas = canvas
                container._heatmap_figure = fig
            except Exception:
                pass
            try:
                container.update()
            except Exception:
                pass
            # DO NOT close the figure - we need it to stay rendered in the canvas!

            # --- Interactivity: hover annotation and click-to-select ---
            annot = ax.annotate("", xy=(0,0), xytext=(15,15), textcoords="offset points",
                                bbox=dict(boxstyle="round", fc="w"), arrowprops=dict(arrowstyle="->"))
            annot.set_visible(False)

            def update_annot(i_idx, j_idx):
                try:
                    col = disp.columns[j_idx]
                    horse = disp.index[i_idx]
                    val = disp.values[i_idx, j_idx]
                    text = f"{horse}\n{col}: {val:.2f}"
                    annot.xy = (j_idx, i_idx)
                    annot.set_text(text)
                    annot.get_bbox_patch().set_alpha(0.9)
                except Exception:
                    annot.set_visible(False)

            def on_move(event):
                if event.inaxes != ax:
                    if annot.get_visible():
                        annot.set_visible(False)
                        canvas.draw_idle()
                    return
                if event.xdata is None or event.ydata is None:
                    return
                j = int(round(event.xdata))
                i = int(round(event.ydata))
                if 0 <= i < len(disp.index) and 0 <= j < len(disp.columns):
                    update_annot(i, j)
                    if not annot.get_visible():
                        annot.set_visible(True)
                    canvas.draw_idle()
                else:
                    if annot.get_visible():
                        annot.set_visible(False)
                        canvas.draw_idle()

            def on_click(event):
                if event.inaxes != ax:
                    return
                if event.xdata is None or event.ydata is None:
                    return
                j = int(round(event.xdata))
                i = int(round(event.ydata))
                if 0 <= i < len(disp.index) and 0 <= j < len(disp.columns):
                    horse = str(disp.index[i])
                    try:
                        if parent and hasattr(parent, 'select_horse_by_name'):
                            parent.select_horse_by_name(horse)
                    except Exception:
                        pass

            # connect events
            try:
                canvas.mpl_connect('motion_notify_event', on_move)
                canvas.mpl_connect('button_press_event', on_click)
            except Exception:
                pass

            print("[DEBUG] Matplotlib heatmap rendered successfully with interactivity")
        except Exception as e:
            print(f"[ERROR] render_plot failed: {e}")
            import traceback
            traceback.print_exc()

    # export handler
    def save_png():
        try:
            if last_fig is None:
                QMessageBox.warning(container, "No Figure", "No heatmap rendered yet.")
                return
            # Generate dynamic filename from date and race_id if available from parent
            default_filename = "heatmap.png"
            if parent and hasattr(parent, 'date_filter') and hasattr(parent, 'race_id_filter'):
                race_date = parent.date_filter.currentText().strip()
                race_id = parent.race_id_filter.currentText().strip()
                if race_date and race_id:
                    date_clean = race_date.replace('/', '')
                    default_filename = f"{date_clean}_{race_id}_heatmap.png"
                elif race_date:
                    date_clean = race_date.replace('/', '')
                    default_filename = f"{date_clean}_heatmap.png"
            
            path, _ = QFileDialog.getSaveFileName(container, "Save heatmap PNG", default_filename, 
                                                 "PNG Files (*.png);;All Files (*)")
            if path:
                last_fig.savefig(path, dpi=150, bbox_inches='tight')
                QMessageBox.information(container, "Saved", f"Heatmap PNG saved to {path}")
                print(f"[INFO] Heatmap saved to {path}")
        except Exception as e:
            print(f"[ERROR] Failed to save heatmap PNG: {e}")
            QMessageBox.critical(container, "Error", f"Failed to save heatmap: {e}")

    # connect signals (only if widgets exist)
    # connect checkbox state changes to re-render
    try:
        for cb in metrics_checkboxes:
            def _on_checkbox_changed(state, checkbox=cb):
                try:
                    metric_name = checkbox.text()
                    is_checked = checkbox.isChecked()
                    if callable(on_metric_toggle):
                        on_metric_toggle(metric_name, is_checked)
                    render_plot()
                except Exception as e:
                    print(f"[ERROR] Checkbox change handler failed: {e}")
            cb.stateChanged.connect(_on_checkbox_changed)
    except Exception:
        pass
    try:
        if sort_combo is not None and hasattr(sort_combo, 'currentIndexChanged'):
            sort_combo.currentIndexChanged.connect(lambda _: render_plot())
    except Exception:
        pass
    try:
        if vmin_spin is not None and hasattr(vmin_spin, 'valueChanged'):
            vmin_spin.valueChanged.connect(lambda _: render_plot())
    except Exception:
        pass
    try:
        if vmax_spin is not None and hasattr(vmax_spin, 'valueChanged'):
            vmax_spin.valueChanged.connect(lambda _: render_plot())
    except Exception:
        pass
    try:
        if cmap_combo is not None and hasattr(cmap_combo, 'currentIndexChanged'):
            cmap_combo.currentIndexChanged.connect(lambda _: render_plot())
    except Exception:
        pass
    try:
        if export_btn is not None and hasattr(export_btn, 'clicked'):
            export_btn.clicked.connect(save_png)
    except Exception:
        pass
    print("[DEBUG] Signal connections made (conditional)")

    # initial render
    print("[DEBUG] Calling initial render_plot")
    render_plot()
    print("[DEBUG] create_interactive_matplotlib_heatmap completed successfully")
    return container


def create_heatmap_controls(norm_df=None, parent=None):
    """Create a standalone controls widget for the heatmap and return (widget, controls_dict).

    controls_dict contains: 'sort_combo','vmin_spin','vmax_spin','cmap_combo','export_btn'
    If norm_df is provided, column names are added to the sort dropdown.
    """
    if not PYQT_AVAILABLE:
        raise ImportError("PyQt5 is required for the interactive heatmap GUI but is not installed.")
    controls = QWidget(parent)
    layout = QHBoxLayout(controls)
    layout.setContentsMargins(4, 4, 4, 4)

    sort_combo = QComboBox()
    sort_combo.addItem('Composite')
    if norm_df is not None:
        for col in norm_df.columns:
            sort_combo.addItem(col)
    sort_combo.setMaximumWidth(180)

    vmin_spin = QDoubleSpinBox()
    vmax_spin = QDoubleSpinBox()
    vmin_spin.setRange(0.0, 1.0)
    vmax_spin.setRange(0.0, 1.0)
    vmin_spin.setSingleStep(0.01)
    vmax_spin.setSingleStep(0.01)
    vmin_spin.setValue(0.0)
    vmax_spin.setValue(1.0)
    vmin_spin.setMaximumWidth(80)
    vmax_spin.setMaximumWidth(80)

    cmap_combo = QComboBox()
    cmap_combo.addItems(['RdYlGn', 'Viridis', 'Plasma', 'Cividis'])
    cmap_combo.setMaximumWidth(140)

    export_btn = QPushButton("Export PNG")
    export_btn.setMaximumWidth(100)

    # apply styles where possible
    try:
        controls.setStyleSheet(CONTROL_STYLE)
        sort_combo.setStyleSheet(CONTROL_STYLE)
        vmin_spin.setStyleSheet(CONTROL_STYLE)
        vmax_spin.setStyleSheet(CONTROL_STYLE)
        cmap_combo.setStyleSheet(CONTROL_STYLE)
        export_btn.setObjectName('exportBtn')
        export_btn.setStyleSheet(CONTROL_STYLE)
    except Exception:
        pass

    layout.addWidget(QLabel('Sort by:'))
    layout.addWidget(sort_combo)
    layout.addWidget(QLabel('vmin'))
    layout.addWidget(vmin_spin)
    layout.addWidget(QLabel('vmax'))
    layout.addWidget(vmax_spin)
    layout.addWidget(QLabel('Colormap'))
    layout.addWidget(cmap_combo)
    layout.addWidget(export_btn)

    controls_dict = {
        'sort_combo': sort_combo,
        'vmin_spin': vmin_spin,
        'vmax_spin': vmax_spin,
        'cmap_combo': cmap_combo,
        'export_btn': export_btn,
    }
    return controls, controls_dict
