import asyncio
import threading
from bleak import BleakScanner, BleakClient
import tkinter as tk
from tkinter import scrolledtext, messagebox, filedialog, ttk
import struct
import csv
import sys
import os
import time
from datetime import datetime
import queue
import re

# Check bleak version for compatibility
try:
    import bleak
    BLEAK_VERSION = bleak.__version__
except:
    BLEAK_VERSION = "unknown"

# Expected data structures from STM32 (Updated with correct sizes):
# 
# Telemery_SensorData (20 bytes):
# typedef struct __attribute__((packed)){
#     uint32_t timestamp_s;     			// Unix timestamp (seconds) - 4 bytes
#     uint16_t timestamp_ms;			    // Milliseconds part (0-999) - 2 bytes
#     uint16_t pulse_count;              // 0-50,000 - 2 bytes
#     uint16_t charge_count;		    // Charge counter (0-25,000) - 2 bytes
#     uint16_t min_pulse;                // 1-10,000 - 2 bytes
# 	uint16_t max_pulse;			    // 0-50,000 - 2 bytes
#     uint16_t adc_value;       			// ADC reading (0-3,000) - 2 bytes
#     uint16_t pulseMean_scaled;         // Pulse mean * 100 (fixed point) - 2 bytes
#     uint16_t std_Deviation_scaled;     // Standard deviation * 100 (fixed point) - 2 bytes
# } Telemery_SensorData;
# 
# RamInfo (10 bytes):
# typedef struct __attribute__((packed)) {
#     uint32_t total_RAM;        // 4 bytes
#     uint16_t used_RAM;         // 2 bytes
#     float usage_Percent;       // 4 bytes
# } RamInfo;
# 
# FlashInfo (22 bytes):
# typedef struct __attribute__((packed)) {   
#     uint16_t page_size;        // 2 bytes
#     uint16_t pagesPerBlock;    // 2 bytes
#     uint16_t blockCount;       // 2 bytes
#     uint16_t dataBlockStarts;  // 2 bytes
#     uint16_t dataBlockEnds;    // 2 bytes
#     uint16_t reservedSystemBlock; // 2 bytes
#     uint16_t calibPage;        // 2 bytes
#     uint16_t diagPage;         // 2 bytes
#     uint16_t sysConfigPage;    // 2 bytes
#     uint16_t totalFlashSize;   // 2 bytes
#     uint16_t usedSize;         // 2 bytes
# } FlashInfo;
# 
# DiagnosticInfo (75 bytes):
# typedef struct __attribute__((packed)) {
#     char serialNumber[20];     // 20 bytes
#     char firmwareVersion[16];  // 16 bytes
#     uint16_t flashEraseCycles; // 2 bytes
#     uint16_t Interrupt_interval; // 2 bytes
#     bool overUSB;              // 1 byte
#     char buildDate[16];        // 16 bytes
#     int16_t d_mcu_Temp;        // 2 bytes
#     int16_t d_amb_Temp;        // 2 bytes
#     uint16_t d_battery_voltage; // 2 bytes
#     uint8_t reserved[4];       // 4 bytes
#     uint32_t crc32;            // 4 bytes
#     char magic[4];             // 4 bytes
# } DiagnosticInfo;
# 
# BattInfo (4 bytes):
# typedef struct __attribute__((packed)) {
#     uint16_t batt_Volt;        // 2 bytes - Battery voltage in mV
#     uint16_t batt_ADC;         // 2 bytes - Raw ADC value (0-4095 for 12-bit ADC)
# } BattInfo;
#
# TempInfo (8 bytes):
# typedef struct __attribute__((packed)) {
#     float s_amb_temp;          // 4 bytes - Ambient temperature
#     float s_mcu_temp;          // 4 bytes - MCU temperature
# } TempInfo;
#
# Sys_Config (19 bytes):
# typedef struct __attribute__((packed)) {
#     uint16_t Interrupt_interval;   // 2 bytes - Interrupt interval setting
#     bool overUSB;                  // 1 byte - USB communication flag
#     uint32_t crc32;                // 4 bytes - CRC32 checksum
#     char reserved[4];              // 4 bytes - Reserved space
#     uint8_t livedata;              // 1 byte - Live data flag
#     uint8_t is_Sensor;             // 1 byte - Sensor enabled flag
#     uint8_t indolence;             // 1 byte - Indolence timeout (minutes)
#     bool is_Shipment;              // 1 byte - Shipment mode flag
#     char magic[4];                // 4 bytes - Magic string "CB100"
# } Sys_Config;


# P2P Notify Characteristic UUID from your STM32WB project
CHAR_UUID = "0000fe42-0000-1000-8000-00805f9b34fb"  # P2P_NOTIFY_CHAR_UUID

# Telemetry Prefix Definitions (matching STM32 firmware)
TELEMETRY_PREFIX_SENSOR_DATA = 0x01      # Telemetry_SensorData (20 bytes)
TELEMETRY_PREFIX_DIAGNOSTIC = 0x02       # Diagnostic info (75 bytes)
TELEMETRY_PREFIX_RAM_INFO = 0x03         # RAM info (10 bytes)
TELEMETRY_PREFIX_FLASH_INFO = 0x04       # Flash info (22 bytes)
TELEMETRY_PREFIX_BATTERY_INFO = 0x05     # Battery info (4 bytes)
TELEMETRY_PREFIX_TEMPERATURE = 0x06      # Temperature data (8 bytes)
TELEMETRY_PREFIX_SYSTEM_CONFIG = 0x07    # System config (19 bytes)
TELEMETRY_PREFIX_FIRMWARE = 0x08         # Firmware version info (31 bytes)
TELEMETRY_PREFIX_TEXT_RESPONSE = 0x09    # Text response (variable bytes)

class RDCScannerApp:
    def __init__(self, root):
        self.root = root
        # --- buttons position changed, added help button, added temperature monitoring, added battery monitoring 
        # --- added firmware version in Device statistics, 
        self.root.title("CB100 BLE Scanner version 2.1.7")        
        
        # Store multiple clients: {address: {'client': BleakClient, 'name': device.name, 'notify_char_uuid': uuid}}
        self.clients = {}

        # Data statistics for each device
        self.device_stats = {}  # {address: {'total_pulses': 0, 'total_charge': 0, 'battery_voltage': None, 'ambient_temperature': None, 'firmware_version': None}}
        
        # Track last logged battery voltage to prevent duplicate logs
        self.last_logged_battery = {}  # {address: {'voltage': int, 'timestamp': float}}
        
        # Track last logged temperature to prevent duplicate logs
        self.last_logged_temperature = {}  # {address: {'amb_temp': float, 'mcu_temp': float}}
        
        # Color scheme for device statistics - different color for each device (darker colors)
        self.device_colors = [
            '#CC0000',  # Dark Red
            '#008B8B',  # Dark Turquoise
            '#1E90FF',  # Dark Blue
            '#CD5C5C',  # Dark Salmon
            '#66CDAA',  # Dark Mint
            '#DAA520',  # Dark Yellow/Gold
            '#9370DB',  # Dark Purple
            '#4682B4',  # Dark Sky Blue
            '#CD853F',  # Dark Peach
        ]
        self.device_color_map = {}  # {address: color} - maps device address to assigned color
        
        # Structured sensor data storage for CSV export
        self.sensor_data_records = []  # List of sensor data dictionaries
        
        # Control variable for sensor data processing
        self.process_sensor_data = True  # Set to True to enable processing 20-byte sensor data (enabled by default)
        
        # Auto-reconnect tracking: {address: {'auto_reconnect': bool, 'original_device': device, 'reconnect_attempts': int}}
        self.auto_reconnect_info = {}
        
        # Ubuntu/Linux-specific initialization
        import platform
        self.is_linux = platform.system().lower() == 'linux'
        
        # Store icon path for popup windows
        self.icon_path = os.path.join(os.path.dirname(__file__), "inphys.ico")
        if not os.path.exists(self.icon_path):
            self.icon_path = None
        
        # Dialog message system for sensor data monitoring
        self.dialog_queue = queue.Queue()
        self.last_sensor_data_time = {}  # Track last sensor data time per device
        self.data_timeout_threshold = 0.3  # 300ms timeout
        self.dialog_monitor_running = False
        self.timeout_popup = None  # Persistent popup window for timeout records
        self.timeout_records = []  # Store timeout records
        
        # Data rate limiting to prevent burst overload
        self.data_rate_limiter = {}  # Track data rate per device
        self.max_data_rate = 10  # Maximum 10 packets per second per device
        
        # Data loss detection
        self.data_loss_stats = {}  # Track data loss per device
        self.last_sequence_number = {}  # Track sequence numbers per device
        
        # Battery monitoring
        self.battery_monitoring_active = False
        self.battery_monitoring_interval = 600  # Default 10 minutes (600 seconds)
        self.battery_monitoring_job = None  # Store scheduled job ID
        
        # Temperature monitoring
        self.temperature_monitoring_active = False
        self.temperature_monitoring_interval = 600  # Default 10 minutes (600 seconds)
        self.temperature_monitoring_job = None  # Store scheduled job ID

        # --- Add a label to show the current device ---
        # self.device_label = tk.Label(root, text="No Device Connected", font=("Arial", 12))
        # self.device_label.pack(pady=5)

        # --- Create main container with left and right panels ---
        main_container = tk.Frame(root)
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Left panel for buttons
        left_panel = tk.Frame(main_container)
        left_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        
        # Right panel for connected devices - align with left panel height
        right_panel = tk.Frame(main_container)
        right_panel.pack(side=tk.RIGHT, fill=tk.BOTH, padx=(5, 0))
        
        # --- Right panel: Connected Devices section ---
        device_section = tk.LabelFrame(right_panel, text="Connected Devices", padx=10, pady=5)
        device_section.pack(fill=tk.BOTH, expand=False, pady=(0, 10))
        
        self.connected_listbox = tk.Listbox(device_section, width=18, height=9, selectmode=tk.MULTIPLE)
        self.connected_listbox.pack(fill=tk.BOTH, expand=True, pady=(0, 5))
        self.connected_listbox.bind('<<ListboxSelect>>', self.on_connected_listbox_select)
        
        # Buttons next to connected device list (on the right)
        self.search_btn = tk.Button(device_section, text="Search Device", command=self.search_devices, width=20)
        self.search_btn.pack(fill=tk.X, pady=(0, 3))
        
        self.quick_connect_btn = tk.Button(device_section, text="Quick Connect", command=self.quick_connect, width=20)
        self.quick_connect_btn.pack(fill=tk.X, pady=(0, 3))
        
        self.disconnect_btn = tk.Button(device_section, text="Disconnect Selected", command=self.disconnect_selected, state=tk.DISABLED, width=20)
        self.disconnect_btn.pack(fill=tk.X, pady=(0, 3))
        
        self.force_stop_btn = tk.Button(device_section, text="Force Stop All", command=self.force_stop_all_notifications, width=20, bg="red", fg="white")
        self.force_stop_btn.pack(fill=tk.X)
        
        # Method to update the device label with the connected device's name
        # def update_device_label(name):
        #     self.device_label.config(text=f"Connected Device: {name}")

        # self.update_device_label = update_device_label

        self.device_list = []
        self.client = None
        self.connected = False
        self.data_buffer = []  # Store parsed data for CSV
        self.notify_char_uuid = None
        
        # Data buffering for diagnostic info (65 bytes)
        self.diagnostic_buffer = {}  # Store partial diagnostic data per device
        self.diagnostic_buffer_time = {}  # Track when buffer was last updated
        
        
        # Add status tracking for data reception
        self.last_data_time = {}
        self.status_checker_running = False

        # --- Left panel: All other buttons and content ---
        # Top Frame for buttons on left side
        top_frame = tk.Frame(left_panel)
        top_frame.pack(fill=tk.X, pady=(0, 5))

        # First row of buttons - Main actions
        top_row1 = tk.Frame(top_frame)
        top_row1.pack(fill=tk.X, pady=(0, 5))
        
        self.sensor_toggle_btn = tk.Button(top_row1, text="Stop Sensor Data", command=self.toggle_sensor_data, width=15, bg="orange")
        self.sensor_toggle_btn.pack(side=tk.LEFT, padx=(0, 5))
        
        self.send_command_btn = tk.Button(top_row1, text="Send Command", command=self.send_custom_command, width=15)
        self.send_command_btn.pack(side=tk.LEFT, padx=5)
        
        self.save_btn = tk.Button(top_row1, text="Save Data", command=self.save_data, width=15)
        self.save_btn.pack(side=tk.LEFT, padx=5)
        
        self.save_log_btn = tk.Button(top_row1, text="Save Log", command=self.save_data_log, width=15)
        self.save_log_btn.pack(side=tk.LEFT, padx=5)
        
        self.clear_btn = tk.Button(top_row1, text="Clear", command=self.clear_console, width=15)
        self.clear_btn.pack(side=tk.LEFT, padx=5)
        
        # Second row of buttons - Control buttons
        top_row2 = tk.Frame(top_frame)
        top_row2.pack(fill=tk.X, pady=(0, 5))
        
        self.status_checker_btn = tk.Button(top_row2, text="Start Status Check", command=self.toggle_status_checker, width=15)
        self.status_checker_btn.pack(side=tk.LEFT, padx=(0, 5))
        
        self.refresh_ui_btn = tk.Button(top_row2, text="Refresh UI", command=self.force_refresh_ui, width=15)
        self.refresh_ui_btn.pack(side=tk.LEFT, padx=5)
        
        self.dialog_toggle_btn = tk.Button(top_row2, text="Start Dialogs", command=self.toggle_dialog_monitoring, width=15, bg="lightgreen")
        self.dialog_toggle_btn.pack(side=tk.LEFT, padx=5)
        
        # Add battery monitoring interval button
        self.battery_monitor_btn = tk.Button(top_row2, text="Battery Monitor (OFF)", command=self.set_battery_monitor_interval, width=20, bg="gray", fg="white")
        self.battery_monitor_btn.pack(side=tk.LEFT, padx=5)
        
        # Add temperature monitoring interval button
        self.temperature_monitor_btn = tk.Button(top_row2, text="Temp Monitor (OFF)", command=self.set_temperature_monitor_interval, width=20, bg="gray", fg="white")
        self.temperature_monitor_btn.pack(side=tk.LEFT, padx=5)
        
        # Add Help button to show API command list
        self.help_btn = tk.Button(top_row2, text="Help", command=self.show_api_help, width=15)
        self.help_btn.pack(side=tk.LEFT, padx=5)
        
        # Add Ubuntu help button if on Linux
        if self.is_linux:
            self.ubuntu_help_btn = tk.Button(top_row2, text="Ubuntu Help", command=self.show_ubuntu_help, width=15)
            self.ubuntu_help_btn.pack(side=tk.LEFT, padx=5)

        # --- Statistics Frame ---
        stats_frame = tk.LabelFrame(left_panel, text="Device Statistics", padx=10, pady=5)
        stats_frame.pack(fill=tk.X, pady=(0, 5))
        
        # Create a treeview for statistics
        self.stats_tree = ttk.Treeview(stats_frame, columns=("Device", "Firmware Version", "Total Pulses", "Total Charge", "Battery Voltage", "Ambient Temperature"), show="headings", height=9)
        self.stats_tree.heading("Device", text="Device", anchor="w")
        self.stats_tree.heading("Firmware Version", text="Firmware Version", anchor="w")
        self.stats_tree.heading("Total Pulses", text="Total Pulses", anchor="e")
        self.stats_tree.heading("Total Charge", text="Total Charge", anchor="e")
        self.stats_tree.heading("Battery Voltage", text="Battery Voltage (mV)", anchor="e")
        self.stats_tree.heading("Ambient Temperature", text="Ambient Temperature (°C)", anchor="e")
        
        # Set column widths and alignment
        self.stats_tree.column("Device", width=150, anchor="w")
        self.stats_tree.column("Firmware Version", width=150, anchor="w")
        self.stats_tree.column("Total Pulses", width=120, anchor="e")
        self.stats_tree.column("Total Charge", width=120, anchor="e")
        self.stats_tree.column("Battery Voltage", width=150, anchor="e")
        self.stats_tree.column("Ambient Temperature", width=160, anchor="e")
        
        self.stats_tree.pack(fill=tk.X)
        
        # Configure color tags for device statistics rows (darker colors, normal font)
        for i, color in enumerate(self.device_colors):
            tag_name = f"device_color_{i}"
            # Make colors darker with normal font
            self.stats_tree.tag_configure(tag_name, foreground=color)

        # --- Console in the middle ---
        console_frame = tk.LabelFrame(left_panel, text="Data Log", padx=10, pady=5)
        console_frame.pack(fill=tk.BOTH, expand=True)
        
        self.console = scrolledtext.ScrolledText(console_frame, width=152, height=14, state=tk.NORMAL, font=("Consolas", 9))
        self.console.pack(fill=tk.BOTH, expand=True)


        # Start periodic stats update
        self.update_stats_display()
        
        # Log startup message
        self.log("CB100 BLE Scanner started. Click 'Search Device' to begin scanning for CB100 devices.")
        self.log("Sensor data processing is ENABLED by default. Sensor data will be processed and displayed.")
        self.log("Click 'Stop Sensor Data' to disable sensor data processing.")
        
        # Ubuntu-specific initialization and system check (after UI is created)
        if self.is_linux:
            self.check_ubuntu_ble_system()
        
        # Add proper shutdown handling
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Dialog monitoring system starts in stopped state by default

    def check_ubuntu_ble_system(self):
        """Check Ubuntu BLE system requirements and provide recommendations"""
        try:
            import subprocess
            import shutil
            
            self.log("=== Ubuntu BLE System Check ===")
            
            # Check if bluetoothctl is available
            if shutil.which('bluetoothctl'):
                self.log("✓ bluetoothctl found")
            else:
                self.log("⚠ bluetoothctl not found - install bluez package")
            
            # Check if hcitool is available
            if shutil.which('hcitool'):
                self.log("✓ hcitool found")
            else:
                self.log("⚠ hcitool not found - install bluez package")
            
            # Check if hciconfig is available
            if shutil.which('hciconfig'):
                self.log("✓ hciconfig found")
            else:
                self.log("⚠ hciconfig not found - install bluez package")
            
            # Check Bluetooth service status
            try:
                result = subprocess.run(['systemctl', 'is-active', 'bluetooth'], 
                                      capture_output=True, text=True, timeout=5)
                if result.stdout.strip() == 'active':
                    self.log("✓ Bluetooth service is active")
                else:
                    self.log("⚠ Bluetooth service is not active - run: sudo systemctl start bluetooth")
            except:
                self.log("⚠ Could not check Bluetooth service status")
            
            # Check if user is in bluetooth group
            try:
                import getpass
                import grp
                user = getpass.getuser()
                bluetooth_group = grp.getgrnam('bluetooth')
                if user in bluetooth_group.gr_mem:
                    self.log("✓ User is in bluetooth group")
                else:
                    self.log("⚠ User not in bluetooth group - run: sudo usermod -a -G bluetooth $USER")
            except:
                self.log("⚠ Could not check bluetooth group membership")
            
            # Check for BlueZ D-Bus issues
            try:
                result = subprocess.run(['dbus-send', '--system', '--dest=org.bluez', 
                                       '--print-reply', '--type=method_call', 
                                       '/org/bluez', 'org.freedesktop.DBus.Introspectable.Introspect'], 
                                      capture_output=True, text=True, timeout=5)
                if result.returncode == 0:
                    self.log("✓ BlueZ D-Bus interface is accessible")
                else:
                    self.log("⚠ BlueZ D-Bus interface may have issues")
                    self.log("  Try: sudo systemctl restart bluetooth")
            except:
                self.log("⚠ Could not check BlueZ D-Bus interface")
            
            # Check for common Ubuntu BLE issues
            self.log("=== Ubuntu BLE Troubleshooting Tips ===")
            self.log("If scanning fails with BlueZ D-Bus errors:")
            self.log("1. Restart Bluetooth: sudo systemctl restart bluetooth")
            self.log("2. Check adapter: sudo hciconfig")
            self.log("3. Reset adapter: sudo hciconfig hci0 reset")
            self.log("4. Test with bluetoothctl: bluetoothctl")
            self.log("5. Check permissions: ls -la /var/lib/bluetooth/")
            
            self.log("=== End Ubuntu BLE System Check ===")
            
        except Exception as e:
            self.log(f"Error during Ubuntu BLE system check: {e}")

    def set_popup_icon(self, popup):
        """Set icon for popup windows"""
        try:
            if self.icon_path:
                popup.iconbitmap(self.icon_path)
        except Exception as e:
            # Silent fail if icon cannot be set
            pass
    
    def log(self, msg):
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        formatted_msg = f"[{timestamp}] {msg}"
        
        # Check if console exists before trying to use it
        if hasattr(self, 'console'):
            # Check if user is at the bottom before inserting new message
            is_at_bottom = self.console.yview()[1] >= 0.99  # Check if scrolled to bottom (within 1%)
            
            self.console.config(state=tk.NORMAL)
            self.console.insert(tk.END, formatted_msg + "\n")
            
            # Only auto-scroll if user was already at the bottom
            if is_at_bottom:
                self.console.see(tk.END)
            
            self.console.config(state=tk.NORMAL)
        else:
            # Fallback to print if console not yet created
            print(formatted_msg)
        
        # Also store the log message for saving
        if hasattr(self, 'data_buffer'):
            self.data_buffer.append(formatted_msg)

    def format_data_for_display(self, data):
        """Format data for better ASCII display"""
        try:
            # Try to decode as ASCII first
            ascii_data = data.decode('ascii', errors='replace')
            
            # Replace non-printable characters with readable representations
            formatted_data = ""
            for char in ascii_data:
                if 32 <= ord(char) <= 126:  # Printable ASCII
                    formatted_data += char
                elif char == '\0':  # Null character
                    formatted_data += "\\0"
                elif char == '\n':  # Newline
                    formatted_data += "\\n"
                elif char == '\r':  # Carriage return
                    formatted_data += "\\r"
                elif char == '\t':  # Tab
                    formatted_data += "\\t"
                elif ord(char) == 0xFFFD:  # Unicode replacement character
                    # Find the original byte and show it as hex
                    byte_index = len(formatted_data)
                    if byte_index < len(data):
                        formatted_data += f"\\x{data[byte_index]:02x}"
                    else:
                        formatted_data += "\\xfffd"
                else:  # Other non-printable
                    formatted_data += f"\\x{ord(char):02x}"
            
            return formatted_data
        except:
            # Fallback: show each byte as hex
            return " ".join(f"\\x{b:02x}" for b in data)

    def format_data_mixed(self, data):
        """Format data showing both ASCII and hex for mixed content"""
        ascii_part = ""
        hex_part = ""
        
        for i, byte in enumerate(data):
            if 32 <= byte <= 126:  # Printable ASCII
                ascii_part += chr(byte)
                hex_part += f"{byte:02x} "
            else:
                ascii_part += f"\\x{byte:02x}"
                hex_part += f"{byte:02x} "
        
        return f"ASCII: {ascii_part}\nHEX:   {hex_part.strip()}"


    def update_device_stats(self, address, data):
        """Update statistics for a specific device"""
        if address not in self.device_stats:
            self.device_stats[address] = {
                'total_pulses': 0,
                'total_charge': 0,
                'battery_voltage': None,
                'ambient_temperature': None,
                'firmware_version': None
            }
        
        stats = self.device_stats[address]
        stats['total_pulses'] += data['pulse_count']
        stats['total_charge'] += data['charge_count']

    def update_stats_display(self):
        """Update the statistics display in the treeview"""
        # Check if stats_tree exists before trying to use it
        if not hasattr(self, 'stats_tree'):
            return
            
        # Clear existing items
        for item in self.stats_tree.get_children():
            self.stats_tree.delete(item)
        
        # Create copies to avoid modification during iteration
        device_stats_copy = dict(self.device_stats)
        clients_copy = dict(self.clients)
        
        # Get sorted list of addresses for consistent color assignment
        sorted_addresses = sorted(device_stats_copy.keys())
        
        # Add current statistics
        for idx, address in enumerate(sorted_addresses):
            try:
                stats = device_stats_copy[address]
                device_name = clients_copy[address]['name'] if address in clients_copy else "Unknown"
                
                # Assign or get color for this device
                if address not in self.device_color_map:
                    # Assign next available color (cycle if more than 9 devices)
                    color_index = len(self.device_color_map) % len(self.device_colors)
                    self.device_color_map[address] = color_index
                
                color_index = self.device_color_map[address]
                tag_name = f"device_color_{color_index}"
                
                # Show device name only (without MAC address)
                if device_name == address:  # MAC-only device
                    display_name = f"MAC:{address}"
                else:
                    display_name = device_name
                
                # Format battery voltage display
                battery_voltage = stats.get('battery_voltage')
                if battery_voltage is not None:
                    battery_display = f"{battery_voltage} mV ({battery_voltage/1000.0:.2f}V)"
                else:
                    battery_display = "N/A"
                
                # Format ambient temperature display
                ambient_temp = stats.get('ambient_temperature')
                if ambient_temp is not None:
                    temp_display = f"{ambient_temp:.2f}°C"
                else:
                    temp_display = "N/A"
                
                # Format firmware version display
                firmware_version = stats.get('firmware_version')
                if firmware_version is not None:
                    firmware_display = firmware_version
                else:
                    firmware_display = "N/A"
                
                # Insert row with color tag
                self.stats_tree.insert("", "end", values=(
                    display_name,
                    firmware_display,
                    stats['total_pulses'],
                    stats['total_charge'],
                    battery_display,
                    temp_display
                ), tags=(tag_name,))
            except Exception as e:
                # Log error but continue with other devices
                self.log(f"Error updating stats for {address}: {e}")
                continue
        
        # Schedule next update
        self.root.after(1000, self.update_stats_display)

    

    def search_devices(self):
        popup = tk.Toplevel(self.root)
        popup.title("Available CB100 Devices")
        popup.geometry("400x350")
        self.set_popup_icon(popup)
        popup.update()
        center_window(popup, 300, 350)
        listbox = tk.Listbox(popup, width=50, selectmode=tk.MULTIPLE)
        listbox.pack(pady=10, fill=tk.BOTH, expand=True)
        status = tk.Label(popup, text="Scanning...")
        status.pack()

        def scan():
            import asyncio
            from bleak import BleakScanner
            import platform
            from bleak.exc import BleakDBusError

            if not hasattr(self, 'previous_scan_results'):
                self.previous_scan_results = {}

            is_linux = platform.system().lower() == 'linux'
            scan_timeout = 12.0 if is_linux else 8.0

            discovered_devices = {}

            def detection_callback(device, advertisement_data):
                # Only look for CB100 devices
                if device.name and device.name.startswith("CB100-"):
                    discovered_devices[device.address] = {
                        "device": device,
                        "rssi": advertisement_data.rssi if advertisement_data.rssi else -100,
                    }

            try:
                # Try the callback-based approach first
                asyncio.run(BleakScanner.discover(timeout=scan_timeout, detection_callback=detection_callback))
            except BleakDBusError as e:
                self.log(f"BlueZ D-Bus error (callback method): {e}")
                self.log("Falling back to simple discovery method...")
                
                try:
                    # Fallback to simple discovery without callback
                    devices = asyncio.run(BleakScanner.discover(timeout=scan_timeout))
                    
                    # Filter devices manually - only CB100 devices
                    for dev in devices:
                        if dev.name and dev.name.startswith("CB100-"):
                            discovered_devices[dev.address] = {
                                "device": dev,
                                "rssi": -100,  # Default RSSI when not available
                            }
                            
                except Exception as e2:
                    self.log(f"Simple discovery also failed: {e2}")
                    self.log("Trying alternative scanning method...")
                    
                    # Last resort: try with shorter timeout and different approach
                    try:
                        devices = asyncio.run(BleakScanner.discover(timeout=5.0))
                        for dev in devices:
                            if dev.name and dev.name.startswith("CB100-"):
                                discovered_devices[dev.address] = {
                                    "device": dev,
                                    "rssi": -100,
                                }
                    except Exception as e3:
                        self.log(f"All discovery methods failed: {e3}")
                        self.log("Please check Bluetooth service and permissions")
                        return
            except Exception as e:
                self.log(f"Unexpected error during discovery: {e}")
                return

            cb100_devices = [info["device"] for info in discovered_devices.values()]
            self.device_list = cb100_devices

            current_scan_results = {}
            for address, info in discovered_devices.items():
                current_scan_results[address] = {
                    "name": info["device"].name,
                    "address": address,
                    "rssi": info["rssi"],
                }
            self.previous_scan_results = current_scan_results

            if listbox.winfo_exists():
                listbox.delete(0, tk.END)
                for dev in self.device_list:
                    display_name = dev.name if dev.name else f"MAC:{dev.address}"
                    # Display only device name, not MAC address
                    display_text = display_name
                    listbox.insert(tk.END, display_text)

                cb100_count = len(self.device_list)
                if cb100_count > 0:
                    status_text = f"Found {cb100_count} CB100 device(s)"
                else:
                    status_text = "No CB100 devices found. Click Rescan."
                status.config(text=status_text)

                if cb100_count > 0:
                    for i, dev in enumerate(self.device_list):
                        self.log(f"Found CB100 device: {dev.name or 'Unknown'}")

        def threaded_scan():
            status.config(text="Scanning...")
            scan()
        threading.Thread(target=threaded_scan, daemon=True).start()

        def connect_selected():
            idxs = listbox.curselection()
            if not idxs:
                return
            selected_devices = [self.device_list[i] for i in idxs]
            popup.destroy()
            for dev in selected_devices:
                self.connect_to_device(dev)

        def copy_mac_address():
            idxs = listbox.curselection()
            if not idxs:
                return
            selected_device = self.device_list[idxs[0]]
            mac_address = selected_device.address
            popup.clipboard_clear()
            popup.clipboard_append(mac_address)
            copy_btn.config(text="Copied!")
            popup.after(1000, lambda: copy_btn.config(text="Copy MAC"))

        connect_btn = tk.Button(popup, text="Connect", command=connect_selected)
        connect_btn.pack(pady=5)

        copy_btn = tk.Button(popup, text="Copy MAC", command=copy_mac_address)
        copy_btn.pack(pady=2)

        rescan_btn = tk.Button(popup, text="Rescan", command=lambda: threading.Thread(target=threaded_scan, daemon=True).start())
        rescan_btn.pack(pady=5)





    def connect_to_device(self, device):
        import asyncio
        from bleak import BleakClient
        import threading
        import platform

        async def connect_and_listen():
            try:
                # Handle devices with or without names
                device_display_name = device.name if device.name else f"MAC:{device.address}"
                
                # Ubuntu/Linux-specific connection optimizations
                is_linux = platform.system().lower() == 'linux'
                max_retries = 5 if is_linux else 3  # More retries for Linux
                retry_delay = 3 if is_linux else 2  # Longer delay for Linux
                connection_timeout = 20.0 if is_linux else 15.0  # Longer timeout for Linux
                
                for attempt in range(max_retries):
                    try:
                        # Create client with Ubuntu-optimized timeout
                        client = BleakClient(device.address, timeout=15.0 if is_linux else 10.0)
                        
                        # Try to connect with Ubuntu-optimized timeout
                        await asyncio.wait_for(client.connect(), timeout=connection_timeout)
                        
                        # Request larger MTU for better throughput (517 bytes to match STM32 config)
                        try:
                            # Wait a moment for connection to stabilize
                            await asyncio.sleep(0.5)
                            
                            # Try to set MTU (platform-specific)
                            if hasattr(client, '_backend'):
                                # For newer Bleak versions with backend access
                                if hasattr(client._backend, '_mtu_size'):
                                    self.log(f"Current MTU: {client._backend._mtu_size} bytes")
                                
                                # Request MTU exchange (this triggers the negotiation)
                                # The actual MTU will be negotiated between client and server
                                self.log("Requesting MTU exchange for larger packet support...")
                            else:
                                self.log("MTU exchange: Using default (client will negotiate with server)")
                        except Exception as mtu_error:
                            # MTU negotiation is optional, continue even if it fails
                            self.log(f"MTU negotiation skipped: {mtu_error}")
                        
                        break
                        
                    except asyncio.TimeoutError:
                        if attempt < max_retries - 1:
                            if is_linux:
                                self.log(f"Linux BLE connection timeout, retrying in {retry_delay} seconds...")
                            await asyncio.sleep(retry_delay)
                        else:
                            raise Exception("Connection timeout after all retries")
                    except Exception as e:
                        if attempt < max_retries - 1:
                            if is_linux:
                                self.log(f"Linux BLE connection failed: {e}, retrying in {retry_delay} seconds...")
                            await asyncio.sleep(retry_delay)
                        else:
                            raise e
                
                # Store device info, using MAC address as name if no name is available
                device_name_for_storage = device.name if device.name else device.address
                self.clients[device.address] = {'client': client, 'name': device_name_for_storage, 'notify_char_uuid': None}
                
                # Enable auto-reconnect for manually connected devices
                self.auto_reconnect_info[device.address] = {
                    'auto_reconnect': True,
                    'original_device': device,
                    'reconnect_attempts': 0,
                    'max_reconnect_attempts': 10
                }
                
                self.update_connected_listbox()
                self.disconnect_btn.config(state=tk.NORMAL)
                
                # Ubuntu-optimized service discovery with enhanced error handling
                services = None
                service_discovery_method = "unknown"
                
                try:
                    # Method 1: Try the modern approach (bleak >= 0.19.0)
                    services = await client.get_services()
                    service_discovery_method = "modern get_services()"
                except AttributeError as e1:
                    try:
                        # Method 2: Try the older approach (bleak < 0.19.0)
                        await client.get_services()
                        services = client.services
                        service_discovery_method = "legacy get_services()"
                    except AttributeError as e2:
                        try:
                            # Method 3: Try direct access to services
                            services = client.services
                            service_discovery_method = "direct services access"
                        except AttributeError as e3:
                            # Method 4: Try alternative approach
                            try:
                                # Some versions might have services as a property
                                if hasattr(client, 'services'):
                                    services = client.services
                                    service_discovery_method = "services property"
                                else:
                                    raise AttributeError("No services property found")
                            except Exception as e4:
                                # Fallback: Skip service discovery but continue connection
                                services = None
                                service_discovery_method = "skipped (compatibility issue)"
                
                notify_char_uuid = None
                write_char_uuid = None
                service_count = 0
                
                if services is not None:
                    for service in services:
                        service_count += 1
                        for char in service.characteristics:
                            if "notify" in char.properties:
                                # Initialize last data time for this device
                                self.last_data_time[device.address] = time.time()
                                
                                await client.start_notify(char.uuid, lambda sender, data, addr=device.address: self.notification_handler(sender, data, addr))
                                notify_char_uuid = char.uuid
                            elif "write" in char.properties or "write-without-response" in char.properties:
                                write_char_uuid = char.uuid
                else:
                    # Try to use the known characteristic UUID from your STM32 project
                    known_notify_uuid = "0000fe42-0000-1000-8000-00805f9b34fb"  # P2P_NOTIFY_CHAR_UUID
                    
                    try:
                        # Initialize last data time for this device
                        self.last_data_time[device.address] = time.time()
                        await client.start_notify(known_notify_uuid, lambda sender, data, addr=device.address: self.notification_handler(sender, data, addr))
                        notify_char_uuid = known_notify_uuid
                    except Exception as e:
                        self.log(f"Failed to start notifications on known characteristic: {e}")
                
                if notify_char_uuid:
                    self.clients[device.address]['notify_char_uuid'] = notify_char_uuid
                    if write_char_uuid:
                        self.clients[device.address]['write_char_uuid'] = write_char_uuid
                    
                    # Auto-send current date/time to STM32 after successful connection
                    if write_char_uuid:
                        try:
                            # Get current date and time
                            from datetime import datetime
                            current_time = datetime.now()
                            
                            # Format: "Mmm DD YYYY HH:MM:SS" (e.g., "Dec 25 2023 14:30:45")
                            formatted_time = current_time.strftime("%b %d %Y %H:%M:%S")
                            
                            # Create set command
                            set_command = f"set {formatted_time}"
                            cmd_bytes = set_command.encode('utf-8')
                            
                            # Send the command
                            await client.write_gatt_char(write_char_uuid, cmd_bytes)
                            self.log(f"Auto-sent date/time sync to {device_display_name}: {formatted_time}")
                            
                            # Small delay to ensure command is processed
                            await asyncio.sleep(0.5)
                            
                        except Exception as e:
                            self.log(f"Failed to auto-send date/time sync to {device_display_name}: {e}")
                    
                    # Immediately request battery voltage for newly connected device
                    if write_char_uuid:
                        try:
                            await asyncio.sleep(0.3)  # Small delay after time sync
                            await client.write_gatt_char(write_char_uuid, b"batt")
                            self.log(f"Auto-requested battery voltage from {device_display_name}")
                        except Exception as e:
                            self.log(f"Failed to request battery voltage from {device_display_name}: {e}")
                    
                    # Immediately request ambient temperature for newly connected device
                    if write_char_uuid:
                        try:
                            await asyncio.sleep(0.3)  # Small delay after battery request
                            await client.write_gatt_char(write_char_uuid, b"temp amb")
                            self.log(f"Auto-requested ambient temperature from {device_display_name}")
                        except Exception as e:
                            self.log(f"Failed to request temperature from {device_display_name}: {e}")
                    
                    # Immediately request firmware version for newly connected device
                    if write_char_uuid:
                        try:
                            await asyncio.sleep(0.3)  # Small delay after temperature request
                            await client.write_gatt_char(write_char_uuid, b"firmware")
                            self.log(f"Auto-requested firmware version from {device_display_name}")
                        except Exception as e:
                            self.log(f"Failed to request firmware version from {device_display_name}: {e}")
                    
                    # Auto-start status checker if not already running
                    if not self.status_checker_running:
                        self.start_status_checker()
                        self.status_checker_btn.config(text="Stop Status Check")
                    
                    # Auto-start battery monitoring if not already running
                    if not self.battery_monitoring_active:
                        self.start_battery_monitoring()
                    
                    # Auto-start temperature monitoring if not already running
                    if not self.temperature_monitoring_active:
                        self.start_temperature_monitoring()
                
                # Enhanced connection monitoring loop
                while device.address in self.clients:
                    try:
                        # Check if client is still connected
                        is_connected = False
                        if hasattr(client, 'is_connected'):
                            if callable(client.is_connected):
                                is_connected = client.is_connected()
                            else:
                                is_connected = client.is_connected
                        
                        if not is_connected:
                            self.log(f"Device {device_display_name} disconnected unexpectedly")
                            break
                        await asyncio.sleep(1)
                    except Exception as e:
                        self.log(f"Error monitoring connection for {device_display_name}: {e}")
                        break
                    
                # After removal, stop notifications and cleanup
                if notify_char_uuid:
                    try:
                        await client.stop_notify(notify_char_uuid)
                        self.log(f"Stopped notifications for {device_display_name}")
                    except Exception as e:
                        self.log(f"Error stopping notifications for {device_display_name}: {e}")
                
                # Disconnect the client
                try:
                    await client.disconnect()
                    self.log(f"Disconnected from {device_display_name}")
                except Exception as e:
                    self.log(f"Error disconnecting from {device_display_name}: {e}")
                
                # Clean up device-specific data
                if device.address in self.last_data_time:
                    del self.last_data_time[device.address]
                if device.address in self.device_stats:
                    del self.device_stats[device.address]
                if device.address in self.diagnostic_buffer:
                    del self.diagnostic_buffer[device.address]
                if device.address in self.diagnostic_buffer_time:
                    del self.diagnostic_buffer_time[device.address]
                
                # Check if auto-reconnect is enabled for this device
                if device.address in self.auto_reconnect_info and self.auto_reconnect_info[device.address]['auto_reconnect']:
                    self.start_auto_reconnect(device.address)
                
                self.update_connected_listbox()
            except Exception as e:                
                if device.address in self.clients:
                    del self.clients[device.address]
                self.update_connected_listbox()

        threading.Thread(target=lambda: asyncio.run(connect_and_listen()), daemon=True).start()

    def show_manual_mac_connection(self):
        """Show dialog for manual MAC address connection"""
        popup = tk.Toplevel(self.root)
        popup.title("Connect by MAC Address")
        popup.geometry("400x200")
        self.set_popup_icon(popup)
        popup.update()
        center_window(popup, 400, 200)
        
        # MAC address input
        tk.Label(popup, text="Enter BLE MAC Address:").pack(pady=10)
        mac_entry = tk.Entry(popup, width=20, font=('Courier', 12))
        mac_entry.pack(pady=5)
        mac_entry.insert(0, "00:00:00:00:00:00")
        mac_entry.select_range(0, tk.END)
        
        # Status label
        status_label = tk.Label(popup, text="", fg="blue")
        status_label.pack(pady=5)
        
        def connect_by_mac():
            mac_address = mac_entry.get().strip()
            if not mac_address or mac_address == "00:00:00:00:00:00":
                status_label.config(text="Please enter a valid MAC address", fg="red")
                return
            
            # Validate MAC address format (basic check)
            if len(mac_address.split(':')) != 6:
                status_label.config(text="Invalid MAC format. Use XX:XX:XX:XX:XX:XX", fg="red")
                return
            
            status_label.config(text="Connecting...", fg="blue")
            
            # Create a mock device object with the MAC address
            class MockDevice:
                def __init__(self, address):
                    self.address = address
                    self.name = None  # No name for MAC-only connection
            
            mock_device = MockDevice(mac_address)
            popup.destroy()
            self.connect_to_device(mock_device)
        
        # Connect button
        connect_btn = tk.Button(popup, text="Connect", command=connect_by_mac)
        connect_btn.pack(pady=10)
        
        # Cancel button
        cancel_btn = tk.Button(popup, text="Cancel", command=popup.destroy)
        cancel_btn.pack(pady=5)
        
        # Focus on entry and bind Enter key
        mac_entry.focus()
        mac_entry.bind('<Return>', lambda e: connect_by_mac())

    def update_connected_listbox(self):
        try:
            self.connected_listbox.delete(0, tk.END)
            self.listbox_addr_map = []
            
            # Create a copy of clients to avoid modification during iteration
            clients_copy = dict(self.clients)
            
            for addr, info in clients_copy.items():
                try:
                    # Display only device name (no MAC address or auto-reconnect indicator)
                    self.connected_listbox.insert(tk.END, info['name'])
                    self.listbox_addr_map.append(addr)
                except Exception as e:
                    # Log error but continue with other devices
                    self.log(f"Error updating listbox for {addr}: {e}")
                    continue
                    
            self.connected_listbox.selection_clear(0, tk.END)
            self.disconnect_btn.config(state=tk.DISABLED)  # Always disable after update
            
            # Update connection status
            self.update_connection_status()
            
            
        except Exception as e:
            self.log(f"Error in update_connected_listbox: {e}")
            # Force a complete refresh if there's an error
            self.root.after(100, self.force_refresh_ui)

    def update_connection_status(self):
        """Update the connection status display (Connection Status frame removed)"""
        # Connection status frame was removed - information available in Connected Devices list
        pass

    def on_connected_listbox_select(self, event=None):
        selection = self.connected_listbox.curselection()
        if selection and self.connected_listbox.size() > 0:
            self.disconnect_btn.config(state=tk.NORMAL)
        else:
            self.disconnect_btn.config(state=tk.DISABLED)

    def disconnect_selected(self):
        selected_indices = self.connected_listbox.curselection()
        if not selected_indices:
            return
            
        to_disconnect = [self.listbox_addr_map[i] for i in selected_indices]
        
        # Immediately clear selection and disable button
        self.connected_listbox.selection_clear(0, tk.END)
        self.disconnect_btn.config(state=tk.DISABLED)
        
        # Filter out addresses that are no longer in clients to prevent errors
        valid_addresses = [addr for addr in to_disconnect if addr in self.clients]
        
        if valid_addresses:
            # Log the disconnect operation
            device_names = []
            for addr in valid_addresses:
                if addr in self.clients:
                    device_names.append(self.clients[addr]['name'])
            
            
            # Start disconnect process for each valid address
            for addr in valid_addresses:
                self.disconnect_device(addr)
        else:
            # If no valid addresses, just refresh the UI
            self.update_connected_listbox()

    def disconnect_device(self, address):
        import asyncio
        import threading
        
        # Immediately remove from UI to prevent multiple disconnect attempts
        if address in self.clients:
            device_name = self.clients[address]['name']
        
        async def disconnect_async():
            # Check if address still exists in clients before proceeding
            if address not in self.clients:
                # Schedule UI update on main thread
                self.root.after(0, self.update_connected_listbox)
                return  # Already disconnected
            
            # Get device info before removing from clients
            device_info = self.clients[address]
            device_name = device_info['name']
            client = device_info['client']
            notify_char_uuid = device_info.get('notify_char_uuid')
            
            try:
                # Stop notifications if characteristic exists
                if notify_char_uuid:
                    try:
                        await client.stop_notify(notify_char_uuid)
                    except Exception as e:
                        self.log(f"Error stopping notifications for {device_name}: {e}")
                
                # Disconnect the client
                try:
                    await client.disconnect()
                except Exception as e:
                    self.log(f"Error disconnecting client for {device_name}: {e}")
                    
            except Exception as e:
                self.log(f"Error during disconnect process for {device_name}: {e}")
            finally:
                # Always clean up, even if there were errors
                try:
                    # Remove from clients if still exists
                    if address in self.clients:
                        del self.clients[address]
                    
                    # Disable auto-reconnect for manually disconnected devices
                    if address in self.auto_reconnect_info:
                        self.auto_reconnect_info[address]['auto_reconnect'] = False
                        self.log(f"Auto-reconnect disabled for {device_name} (manually disconnected)")
                    
                    # Remove from stats as well
                    if address in self.device_stats:
                        del self.device_stats[address]
                    
                    # Remove from last_data_time if exists
                    if address in self.last_data_time:
                        del self.last_data_time[address]
                    
                    # Clear diagnostic buffer for this device
                    if address in self.diagnostic_buffer:
                        del self.diagnostic_buffer[address]
                    if address in self.diagnostic_buffer_time:
                        del self.diagnostic_buffer_time[address]
                    
                    # Stop battery monitoring if no devices left
                    if not self.clients and self.battery_monitoring_active:
                        self.stop_battery_monitoring()
                    
                    self.log(f"Successfully disconnected from {device_name}.")
                except Exception as cleanup_error:
                    self.log(f"Error during cleanup for {device_name}: {cleanup_error}")
                
                # Schedule UI update on main thread with a small delay to ensure cleanup is complete
                self.root.after(100, self.update_connected_listbox)
                # Also schedule cleanup of orphaned entries
                self.root.after(200, self.cleanup_orphaned_entries)
        
        # Start the async disconnect process
        threading.Thread(target=lambda: asyncio.run(disconnect_async()), daemon=True).start()

    def force_refresh_ui(self):
        """Force a complete refresh of the UI to ensure consistency"""
        try:
            self.log("Force refreshing UI...")
            # Clear all UI elements
            self.connected_listbox.delete(0, tk.END)
            self.listbox_addr_map = []
            
            # Rebuild from current clients state
            for addr, info in self.clients.items():
                try:
                    # Display only device name, not MAC address
                    self.connected_listbox.insert(tk.END, f"{info['name']}")
                    self.listbox_addr_map.append(addr)
                except Exception as e:
                    self.log(f"Error in force refresh for {addr}: {e}")
                    continue
            
            # Update all displays
            self.update_connection_status()
            self.update_stats_display()
            
            self.log("UI refresh completed")
            
        except Exception as e:
            self.log(f"Error in force refresh: {e}")

    def cleanup_orphaned_entries(self):
        """Clean up any orphaned entries in stats and data tracking"""
        try:
            # Remove stats for devices no longer in clients
            orphaned_stats = [addr for addr in self.device_stats.keys() if addr not in self.clients]
            for addr in orphaned_stats:
                del self.device_stats[addr]
                self.log(f"Cleaned up orphaned stats for {addr}")
            
            # Remove last_data_time for devices no longer in clients
            orphaned_data_time = [addr for addr in self.last_data_time.keys() if addr not in self.clients]
            for addr in orphaned_data_time:
                del self.last_data_time[addr]
                self.log(f"Cleaned up orphaned data time for {addr}")
            
            # Remove auto-reconnect info for devices no longer in clients and not set to auto-reconnect
            orphaned_auto_reconnect = []
            for addr in list(self.auto_reconnect_info.keys()):
                if addr not in self.clients and not self.auto_reconnect_info[addr]['auto_reconnect']:
                    orphaned_auto_reconnect.append(addr)
            
            for addr in orphaned_auto_reconnect:
                del self.auto_reconnect_info[addr]
                self.log(f"Cleaned up orphaned auto-reconnect info for {addr}")
                
            if orphaned_stats or orphaned_data_time or orphaned_auto_reconnect:
                self.log(f"Cleanup completed. Removed {len(orphaned_stats)} orphaned stats, {len(orphaned_data_time)} orphaned data times, and {len(orphaned_auto_reconnect)} orphaned auto-reconnect entries.")
                
        except Exception as e:
            self.log(f"Error in cleanup: {e}")

    def disconnect_bt(self):
        self.log("Disconnecting...")
        self.connected = False  # This will break the while loop in connect_and_listen

    def toggle_sensor_data(self):
        """Toggle sensor data processing on/off"""
        self.process_sensor_data = not self.process_sensor_data
        
        if self.process_sensor_data:
            self.sensor_toggle_btn.config(text="Stop Sensor Data", bg="orange")
            self.log("\nSensor data processing ENABLED - 20-byte sensor data will be processed and displayed")
        else:
            self.sensor_toggle_btn.config(text="Start Sensor Data", bg="lightgreen")
            self.log("Sensor data processing DISABLED - sensor data will be silently ignored")

    def save_data(self):
        if not self.sensor_data_records:
            messagebox.showinfo("Info", "No sensor data to save.")
            return
        
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")]
        )
        if filename:
            # Sort by timestamp
            sorted_records = sorted(self.sensor_data_records, key=lambda x: float(x['Timestamp']))
            
            # Write to CSV file
            with open(filename, "w", newline="") as f:
                # Define fieldnames for CSV export (without Device column)
                fieldnames = ['Timestamp', 'Pulses', 'Charge', 'Min_Pulse', 'Max_Pulse', 'ADC', 'Mean', 'Std_Dev']
                
                # Check if any records have extra data
                has_extra_data = any('Extra_Data' in record for record in sorted_records)
                if has_extra_data:
                    fieldnames.append('Extra_Data')
                
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                # Write device name as first line
                if sorted_records:
                    device_name = sorted_records[0]['Device']
                    f.write(f"# Device: {device_name}\n")
                
                writer.writeheader()
                
                # Write data rows
                for record in sorted_records:
                    # Create a clean record for CSV export (exclude Device_Address and Device)
                    csv_record = {k: v for k, v in record.items() if k not in ['Device_Address', 'Device']}
                    writer.writerow(csv_record)
            
            messagebox.showinfo("Saved", f"Saved {len(sorted_records)} sensor data records to {filename}")
        else:
            messagebox.showinfo("Cancelled", "Save operation cancelled.")

    def save_data_log(self):
        """Save the entire data log text to CSV file"""
        if not hasattr(self, 'console'):
            messagebox.showwarning("Warning", "Data log console not available.")
            return
        
        # Get all text from console
        console_text = self.console.get("1.0", tk.END).strip()
        
        if not console_text:
            messagebox.showinfo("Info", "Data log is empty. Nothing to save.")
            return
        
        # Ask user for filename
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("Text files", "*.txt"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                lines = console_text.split('\n')
                
                # Write to CSV file
                with open(filename, "w", newline="", encoding='utf-8') as f:
                    writer = csv.writer(f)
                    
                    # Write header
                    writer.writerow(["Timestamp", "Message"])
                    
                    # Parse and write each line
                    for line in lines:
                        if not line.strip():
                            continue  # Skip empty lines
                        
                        # Parse line: format is "[HH:MM:SS.mmm] message"
                        if line.startswith('[') and ']' in line:
                            timestamp_end = line.index(']')
                            timestamp = line[1:timestamp_end]  # Remove brackets
                            message = line[timestamp_end + 1:].strip()  # Remove ']' and leading space
                            
                            writer.writerow([timestamp, message])
                        else:
                            # Line doesn't match format, save as-is with empty timestamp
                            writer.writerow(["", line])
                
                messagebox.showinfo("Saved", f"Data log saved to {filename}\nTotal lines: {len([l for l in lines if l.strip()])}")
                
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save data log: {e}")
        else:
            messagebox.showinfo("Cancelled", "Save operation cancelled.")

    def notification_handler(self, sender, data, address):
        import platform
        import time
        
        # CRITICAL: Check if device is still connected before processing data
        if address not in self.clients:
            # Device is no longer in clients, ignore this data
            return
        
        # Check if the client is actually connected
        try:
            client = self.clients[address]['client']
            # Try both property and method access for is_connected
            is_connected = False
            if hasattr(client, 'is_connected'):
                if callable(client.is_connected):
                    is_connected = client.is_connected()
                else:
                    is_connected = client.is_connected
            
            if not is_connected:
                # Device is disconnected, remove from clients and ignore data
                self.log(f"Device {address} disconnected, removing from clients")
                if address in self.clients:
                    del self.clients[address]
                self.update_connected_listbox()
                return
        except Exception as e:
            # Error checking connection status, remove device and ignore data
            self.log(f"Error checking connection status for {address}: {e}")
            if address in self.clients:
                del self.clients[address]
            self.update_connected_listbox()
            return
        
        device_name = self.clients[address]['name'] if address in self.clients else "Unknown Device"
        is_linux = platform.system().lower() == 'linux'
        
        # Show MAC address prominently for MAC-only devices
        if device_name == address:  # MAC-only device
            device_display = f"MAC:{address}"
        else:
            device_display = f"{device_name}"
        
        # Check if data has telemetry prefix
        if len(data) > 0:
            prefix = data[0]
            actual_data = data[1:]  # Remove prefix byte
            
            # Debug: Log battery prefix detection
            # if prefix == TELEMETRY_PREFIX_BATTERY_INFO:
            #     self.log(f"DEBUG: Battery prefix (0x{prefix:02X}) detected from {device_display}, data length={len(actual_data)} bytes")
            
            # Handle data based on telemetry prefix
            if prefix == TELEMETRY_PREFIX_SENSOR_DATA:
                self.handle_sensor_data(actual_data, device_display, address)
            elif prefix == TELEMETRY_PREFIX_DIAGNOSTIC:
                self.handle_diagnostic_data(actual_data, device_display)
            elif prefix == TELEMETRY_PREFIX_RAM_INFO:
                self.handle_ram_info(actual_data, device_display)
            elif prefix == TELEMETRY_PREFIX_FLASH_INFO:
                self.handle_flash_info(actual_data, device_display)
            elif prefix == TELEMETRY_PREFIX_BATTERY_INFO:
                self.handle_battery_info(actual_data, device_display, address)
            elif prefix == TELEMETRY_PREFIX_TEMPERATURE:
                self.handle_temperature_data(actual_data, device_display, address)
            elif prefix == TELEMETRY_PREFIX_SYSTEM_CONFIG:
                self.handle_system_config(actual_data, device_display)
            elif prefix == TELEMETRY_PREFIX_FIRMWARE:
                self.handle_firmware_data(actual_data, device_display, address)
            elif prefix == TELEMETRY_PREFIX_TEXT_RESPONSE:
                self.handle_text_response(actual_data, device_display)
            else:
                self.log(f"Unknown telemetry prefix 0x{prefix:02X} from {device_display}")
        else:
            self.log(f"Empty data received from {device_display}")
    
    def handle_sensor_data(self, data, device_display, address):
        """Handle 20-byte sensor data with prefix 0x01"""
        if len(data) == 20:
            # Telemery_SensorData structure (20 bytes)
            # Check if sensor data processing is enabled
            if not hasattr(self, 'process_sensor_data') or not self.process_sensor_data:
                # Silently skip 20-byte sensor data when processing is disabled
                return
            
            # Data rate limiting to prevent burst overload
            current_time = time.time()
            if address not in self.data_rate_limiter:
                self.data_rate_limiter[address] = {'count': 0, 'window_start': current_time}
            
            rate_info = self.data_rate_limiter[address]
            
            # Reset window if more than 1 second has passed
            if current_time - rate_info['window_start'] >= 1.0:
                rate_info['count'] = 0
                rate_info['window_start'] = current_time
            
            # Check if we're exceeding the rate limit
            if rate_info['count'] >= self.max_data_rate:
                # Skip this data packet to prevent overload
                return
            
            # Increment counter
            rate_info['count'] += 1
            
            # Data loss detection based on timestamp jumps
            current_timestamp = struct.unpack('<I', data[0:4])[0]  # Extract timestamp_s
            if address in self.last_sequence_number:
                last_timestamp = self.last_sequence_number[address]
                time_diff = current_timestamp - last_timestamp
                
                # Detect large timestamp jumps (potential data loss)
                if time_diff > 5:  # More than 5 seconds jump
                    if address not in self.data_loss_stats:
                        self.data_loss_stats[address] = {'jumps': 0, 'total_lost_seconds': 0}
                    
                    self.data_loss_stats[address]['jumps'] += 1
                    self.data_loss_stats[address]['total_lost_seconds'] += time_diff
                    
                    self.log(f"DATA_LOSS_DETECTED: {device_display} - {time_diff}s jump detected (Total jumps: {self.data_loss_stats[address]['jumps']})")
            
            self.last_sequence_number[address] = current_timestamp
                
            # Try to parse as Sensor Data
            try:
                # '<IHHHHHHHH' = little-endian: uint32, uint16, uint16, uint16, uint16, uint16, uint16, uint16, uint16
                # Binary data format: timestamp_s(4), timestamp_ms(2), pulse_count(2), charge_count(2), min_pulse(2), max_pulse(2), adc_value(2), pulseMean_scaled(2), std_Deviation_scaled(2)
                # Total expected: 4 + 2 + 2 + 2 + 2 + 2 + 2 + 2 + 2 = 20 bytes
                timestamp_s, timestamp_ms, pulse_count, charge_count, min_pulse, max_pulse, adc_value, pulseMean_scaled, std_Deviation_scaled = struct.unpack('<IHHHHHHHH', data)
                
                # Check for zero-value data first (status/keep-alive messages)
                if (pulse_count == 0 and charge_count == 0 and adc_value == 0 and 
                    min_pulse == 0 and max_pulse == 0):
                    # This looks like a status/keep-alive message
                    
                    # Print status/keep-alive message to main window
                    status_text = f"Status/Keep-Alive Message from {device_display}:\n"
                    status_text += f"Timestamp: {timestamp_s}.{timestamp_ms:03d}\n"
                    status_text += f"Pulse Count: {pulse_count}\n"
                    status_text += f"Charge Count: {charge_count}\n"
                    status_text += f"Min Pulse: {min_pulse}\n"
                    status_text += f"Max Pulse: {max_pulse}\n"
                    status_text += f"ADC Value: {adc_value}\n"
                    status_text += f"Pulse Mean Scaled: {pulseMean_scaled}\n"
                    status_text += f"Std Deviation Scaled: {std_Deviation_scaled}\n"
                    status_text += f"All sensor values are zero\n"
                    self.log(status_text)
                    return
                
                # More strict validation for sensor data
                if (timestamp_s >= 0 and timestamp_s < 4294967295 and  # Any reasonable uint32 timestamp
                    timestamp_ms < 1000 and  # Valid milliseconds
                    pulse_count <= 50000 and  # Reasonable pulse count limit
                    charge_count <= 25000 and  # Reasonable charge count limit  
                    min_pulse <= 10000 and  # Reasonable min pulse limit
                    max_pulse <= 50000 and  # Reasonable max pulse limit
                    adc_value <= 4095 and  # 12-bit ADC limit
                    min_pulse <= max_pulse and  # Min should be <= max
                    (pulse_count > 0 or charge_count > 0 or adc_value > 0)):  # At least one sensor value should be non-zero
                    
                    # Convert small timestamp to proper Unix timestamp
                    # If timestamp is very small (like 1039), it means RTC is not synced to Unix time
                    # We'll add a base offset to make it look like a proper Unix timestamp
                    current_unix_time = int(time.time())
                    
                    # If timestamp is very small (< 1000000), assume it's relative to some base time
                    if timestamp_s < 1000000:
                        # Use current time as base and add the small timestamp as seconds offset
                        # This makes it look like a proper Unix timestamp
                        unix_timestamp = current_unix_time + timestamp_s
                    else:
                        # Already a proper Unix timestamp
                        unix_timestamp = timestamp_s
                    
                    formatted_timestamp = f"{unix_timestamp}.{timestamp_ms:03d}"
                    
                    # Convert scaled values back to float
                    pulseMean = pulseMean_scaled / 100.0
                    std_Deviation = std_Deviation_scaled / 100.0
                    
                    # Create data dictionary for stats
                    data_dict = {
                        'timestamp': formatted_timestamp,
                        'pulse_count': pulse_count,
                        'charge_count': charge_count,
                        'adc_value': adc_value
                    }
                    
                    # Update device statistics
                    self.update_device_stats(address, data_dict)
                    
                    # Store structured sensor data for CSV export
                    sensor_record = {
                        'Timestamp': formatted_timestamp,
                        'Pulses': pulse_count,
                        'Charge': charge_count,
                        'Min_Pulse': min_pulse,
                        'Max_Pulse': max_pulse,
                        'ADC': adc_value,
                        'Mean': pulseMean,
                        'Std_Dev': std_Deviation,
                        'Device': device_display,
                        'Device_Address': address
                    }
                    self.sensor_data_records.append(sensor_record)
                    
                    # Enhanced log message with better formatting and timestamp
                    log_str = f"{device_display}--> TS: {formatted_timestamp:>12} | Pulse: {pulse_count:>6} | Charge: {charge_count:>6} | Min: {min_pulse:>5} | Max: {max_pulse:>5} | ADC: {adc_value:>6} | Mean: {pulseMean:>6.2f} | Std: {std_Deviation:>6.2f}"
                    self.log(log_str)
            
                    # Update last data time for this device
                    current_time = time.time()
                    self.last_data_time[address] = current_time
                    
                    # Update sensor data timestamp for dialog monitoring
                    self.last_sensor_data_time[address] = current_time
                    
                    return
                else:
                    # Check if this might be a status/keep-alive message with all zeros
                    if (pulse_count == 0 and charge_count == 0 and adc_value == 0 and 
                        min_pulse == 0 and max_pulse == 0):
                        # This looks like a status/keep-alive message
                        
                        # Print status/keep-alive message to main window
                        status_text = f"Status/Keep-Alive Message from {device_display}:\n"
                        status_text += f"Timestamp: {timestamp_s}.{timestamp_ms:03d}\n"
                        status_text += f"All sensor values are zero\n"
                        self.log(status_text)
                        return
                    
                    # Data doesn't look like sensor data, try other parsing
                    # Try to decode as text message
                    try:
                        text_data = data.decode('utf-8', errors='ignore').strip()
                        if text_data:
                            # Print 20-byte text message to main window
                            text_msg = f"20-byte Text Message from {device_display}:\n"
                            text_msg += f"Message: {text_data}\n"
                            self.log(text_msg)
                            return
                    except:
                        pass
                    
                    # If all parsing fails, show as unknown binary data
                    # Print unknown 20-byte binary data to main window
                    unknown_text = f"Unknown 20-byte Binary Data from {device_display}:\n"
                    unknown_text += f"Raw Data (Hex): {data.hex()}\n"
                    unknown_text += f"Raw Data (ASCII): {self.format_data_for_display(data)}\n"
                    unknown_text += f"Data Length: {len(data)} bytes"
                    self.log(unknown_text)
                    return
            except struct.error as e:
                # Fall through to general handling
                pass
        else:
            self.log(f"Invalid sensor data length: {len(data)} bytes (expected 20) from {device_display}")

    def handle_diagnostic_data(self, data, device_display):
        """Handle 75-byte diagnostic data with prefix 0x02"""
        if len(data) == 75:
            try:
                # DiagnosticInfo: char[20] + char[16] + uint16 + uint16 + bool + char[16] + int16 + int16 + uint16 + uint8[4] + uint32 + char[4]
                # Format: '<20s16sHHB16shhH4sI4s' (20s=char[20], 16s=char[16], H=uint16, B=bool, h=int16, I=uint32, 4s=char[4])
                serialNumber, firmwareVersion, flashEraseCycles, interrupt_interval, overUSB, buildDate, d_mcu_Temp, d_amb_Temp, d_battery_voltage, reserved, crc32, magic = struct.unpack('<20s16sHHB16shhH4sI4s', data)
                
                # Decode strings and clean them
                serialNumber_str = serialNumber.decode('utf-8', errors='ignore').rstrip('\x00')
                firmwareVersion_str = firmwareVersion.decode('utf-8', errors='ignore').rstrip('\x00')
                buildDate_str = buildDate.decode('utf-8', errors='ignore').rstrip('\x00')
                magic_str = magic.decode('utf-8', errors='ignore').rstrip('\x00')
                
                # Also log to main window
                diagnostic_info_text = f"Diagnostic Information from {device_display}:\n"
                diagnostic_info_text += f"Serial Number: {serialNumber_str}\n"
                diagnostic_info_text += f"Firmware Version: {firmwareVersion_str}\n"
                diagnostic_info_text += f"Flash Erase Cycles: {flashEraseCycles}\n"
                diagnostic_info_text += f"Interrupt Interval: {interrupt_interval} ms\n"
                diagnostic_info_text += f"Over USB: {'Yes' if overUSB else 'No'}\n"
                diagnostic_info_text += f"Build Date: {buildDate_str}\n"
                diagnostic_info_text += f"MCU Temperature: {d_mcu_Temp/100.0:.2f}°C\n"
                diagnostic_info_text += f"Ambient Temperature: {d_amb_Temp/100.0:.2f}°C\n"
                diagnostic_info_text += f"Battery Voltage: {d_battery_voltage} mV\n"
                diagnostic_info_text += f"CRC32: 0x{crc32:08X}\n"
                diagnostic_info_text += f"Magic: {magic_str}\n"
                
                self.log(diagnostic_info_text)
            
            except struct.error as e:
                # If parsing fails, just print the hex data
                self.log(f"Failed to parse 75-byte data as DiagnosticInfo: {e}")
                self.log(f"Raw Data (Hex): {data.hex()}")
        else:
            self.log(f"Invalid diagnostic data length: {len(data)} bytes (expected 75) from {device_display}")

    def handle_ram_info(self, data, device_display):
        """Handle 10-byte RAM info with prefix 0x03"""
        if len(data) == 10:
            try:
                # RamInfo: uint32_t + uint16_t + float = 4 + 2 + 4 = 10 bytes
                # Format: '<IHf' (uint32, uint16, float)
                total_ram, used_ram, usage_percent = struct.unpack('<IHf', data)
                
                # Also log to main window
                ram_info_text = f"RAM Information from {device_display}:\n"
                ram_info_text += f"Total RAM: {total_ram:,} bytes ({total_ram/1024:.1f} KB)\n"
                ram_info_text += f"Used RAM: {used_ram:,} bytes ({used_ram/1024:.1f} KB)\n"
                ram_info_text += f"Free RAM: {total_ram - used_ram:,} bytes ({(total_ram - used_ram)/1024:.1f} KB)\n"
                ram_info_text += f"Used RAM Percentage: {usage_percent:.2f}%\n"
                
                self.log(ram_info_text)
            
            except struct.error as e:
                # If parsing fails, just print the hex data
                self.log(f"Failed to parse 10-byte data as RamInfo: {e}")
                self.log(f"Raw Data (Hex): {data.hex()}")
        else:
            self.log(f"Invalid RAM info length: {len(data)} bytes (expected 10) from {device_display}")

    def handle_flash_info(self, data, device_display):
        """Handle 22-byte Flash info with prefix 0x04"""
        if len(data) == 22:
            try:
                # FlashInfo: 11 * uint16 = 22 bytes
                # Format: '<HHHHHHHHHHH' (11 uint16 values)
                page_size, pagesPerBlock, blockCount, dataBlockStarts, dataBlockEnds, reservedSystemBlock, calibPage, diagPage, sysConfigPage, totalFlashSize, usedSize = struct.unpack('<HHHHHHHHHHH', data)
                
                # Also log to main window
                flash_info_text = f"Flash Information from {device_display}:\n"
                flash_info_text += f"Page Size: {page_size} bytes\n"
                flash_info_text += f"Pages Per Block: {pagesPerBlock}\n"
                flash_info_text += f"Block Count: {blockCount}\n"
                flash_info_text += f"Data Block Starts: {dataBlockStarts}\n"
                flash_info_text += f"Data Block Ends: {dataBlockEnds}\n"
                flash_info_text += f"Reserved System Block: {reservedSystemBlock}\n"
                flash_info_text += f"Calibration Page: {calibPage}\n"
                flash_info_text += f"Diagnostic Page: {diagPage}\n"
                flash_info_text += f"System Config Page: {sysConfigPage}\n"
                flash_info_text += f"Total Flash Size: {totalFlashSize} bytes ({totalFlashSize/1024:.1f} KB)\n"
                flash_info_text += f"Used Size: {usedSize} bytes ({usedSize/1024:.1f} KB)\n"
                flash_info_text += f"Free Size: {totalFlashSize - usedSize} bytes ({(totalFlashSize - usedSize)/1024:.1f} KB)\n"
                
                self.log(flash_info_text)
                
            except struct.error as e:
                # If parsing fails, just print the hex data
                self.log(f"Failed to parse 22-byte data as FlashInfo: {e}")
                self.log(f"Raw Data (Hex): {data.hex()}")
        else:
            self.log(f"Invalid Flash info length: {len(data)} bytes (expected 22) from {device_display}")

    def handle_battery_info(self, data, device_display, address):
        """Handle 4-byte Battery info with prefix 0x05"""
        if len(data) == 4:
            try:
                # BattInfo structure (4 bytes total):
                #   uint16_t batt_Volt (2 bytes) - Battery voltage in mV (e.g., 3760 = 3.76V)
                #   uint16_t batt_ADC (2 bytes) - Raw ADC value (0-4095 for 12-bit ADC)
                # Format: '<HH' = little-endian, 2 uint16 values = 4 bytes total
                batt_volt, batt_adc = struct.unpack('<HH', data)
                
                # Update device statistics with battery voltage
                if address not in self.device_stats:
                    self.device_stats[address] = {
                        'total_pulses': 0,
                        'total_charge': 0,
                        'battery_voltage': None,
                        'ambient_temperature': None,
                        'firmware_version': None
                    }
                self.device_stats[address]['battery_voltage'] = batt_volt
                
                # Log to main window only if voltage changed (prevent duplicate logs)
                # This prevents logging the same voltage value twice in quick succession
                should_log = False
                
                if address not in self.last_logged_battery:
                    # First time receiving battery data from this device - always log
                    should_log = True
                else:
                    last_voltage = self.last_logged_battery[address].get('voltage')
                    # Only log if voltage actually changed
                    if last_voltage != batt_volt:
                        should_log = True
                
                if should_log:
                    # Battery voltage is stored in mV in the firmware
                    battery_info_text = f"Battery Information from {device_display}:\n"
                    battery_info_text += f"Battery Voltage: {batt_volt} mV ({batt_volt/1000.0:.2f}V)\n"
                    # battery_info_text += f"ADC Value: {batt_adc}\n"
                    
                    self.log(battery_info_text)
                    
                    # Update last logged battery voltage
                    if address not in self.last_logged_battery:
                        self.last_logged_battery[address] = {}
                    self.last_logged_battery[address]['voltage'] = batt_volt
                
                # Update statistics display to show new battery voltage value
                self.root.after(0, self.update_stats_display)
            
            except struct.error as e:
                # If parsing fails, just print the hex data
                self.log(f"Failed to parse 4-byte data as BattInfo: {e}")
                self.log(f"Raw Data (Hex): {data.hex()}")
                self.log(f"Raw Data Length: {len(data)} bytes")
        elif len(data) == 2:
            # Handle legacy 2-byte format (for backward compatibility)
            self.log(f"WARNING: Received 2-byte battery data (legacy format) from {device_display}")
            self.log(f"Raw Data (Hex): {data.hex()}")
            self.log(f"Note: Expected 4-byte format (batt_Volt + batt_ADC)")
        else:
            self.log(f"Invalid Battery info length: {len(data)} bytes (expected 4) from {device_display}")
            self.log(f"Raw Data (Hex): {data.hex() if len(data) > 0 else 'empty'}")
            self.log(f"Raw Data Length: {len(data)} bytes")

    def handle_temperature_data(self, data, device_display, address):
        """Handle Temperature data with prefix 0x06 - supports both 4-byte and 8-byte formats"""
        if len(data) == 8:
            try:
                # TempInfo: float + float = 4 + 4 = 8 bytes (both temperatures)
                # Format: '<ff' (float, float)
                s_amb_temp, s_mcu_temp = struct.unpack('<ff', data)
                
                # Update device statistics with ambient temperature
                if address not in self.device_stats:
                    self.device_stats[address] = {
                        'total_pulses': 0,
                        'total_charge': 0,
                        'battery_voltage': None,
                        'ambient_temperature': None,
                        'firmware_version': None
                    }
                self.device_stats[address]['ambient_temperature'] = s_amb_temp
                
                # Log to main window only if temperature changed (prevent duplicate logs)
                should_log = False
                
                if address not in self.last_logged_temperature:
                    # First time receiving temperature data from this device - always log
                    should_log = True
                else:
                    last_temp = self.last_logged_temperature[address]
                    # Only log if ambient temperature or MCU temperature actually changed
                    if (abs(last_temp.get('amb_temp', 0) - s_amb_temp) > 0.01 or 
                        abs(last_temp.get('mcu_temp', 0) - s_mcu_temp) > 0.01):
                        should_log = True
                
                if should_log:
                    # Also log to main window
                    temp_info_text = f"Temperature Information from {device_display}:\n"
                    temp_info_text += f"Ambient Temperature: {s_amb_temp:.2f}°C\n"
                    temp_info_text += f"MCU Temperature: {s_mcu_temp:.2f}°C\n"
                    
                    self.log(temp_info_text)
                    
                    # Update last logged temperature
                    if address not in self.last_logged_temperature:
                        self.last_logged_temperature[address] = {}
                    self.last_logged_temperature[address]['amb_temp'] = s_amb_temp
                    self.last_logged_temperature[address]['mcu_temp'] = s_mcu_temp
                
                # Update statistics display to show new temperature value
                self.root.after(0, self.update_stats_display)
            
            except struct.error as e:
                # If parsing fails, just print the hex data
                self.log(f"Failed to parse 8-byte data as TempInfo: {e}")
                self.log(f"Raw Data (Hex): {data.hex()}")
        elif len(data) == 4:
            try:
                # Single temperature value (4 bytes) - ambient temperature from "temp amb" command
                # Format: '<f' (float)
                s_amb_temp = struct.unpack('<f', data)[0]
                
                # Update device statistics with ambient temperature
                if address not in self.device_stats:
                    self.device_stats[address] = {
                        'total_pulses': 0,
                        'total_charge': 0,
                        'battery_voltage': None,
                        'ambient_temperature': None,
                        'firmware_version': None
                    }
                self.device_stats[address]['ambient_temperature'] = s_amb_temp
                
                # Log to main window only if temperature changed (prevent duplicate logs)
                should_log = False
                
                if address not in self.last_logged_temperature:
                    # First time receiving temperature data from this device - always log
                    should_log = True
                else:
                    last_temp = self.last_logged_temperature[address]
                    # Only log if ambient temperature actually changed
                    if abs(last_temp.get('amb_temp', 0) - s_amb_temp) > 0.01:
                        should_log = True
                
                if should_log:
                    # Log to main window
                    temp_info_text = f"Temperature Information from {device_display}:\n"
                    temp_info_text += f"Ambient Temperature: {s_amb_temp:.2f}°C\n"
                    
                    self.log(temp_info_text)
                    
                    # Update last logged temperature
                    if address not in self.last_logged_temperature:
                        self.last_logged_temperature[address] = {}
                    self.last_logged_temperature[address]['amb_temp'] = s_amb_temp
                
                # Update statistics display to show new temperature value
                self.root.after(0, self.update_stats_display)
            
            except struct.error as e:
                # If parsing fails, just print the hex data
                self.log(f"Failed to parse 4-byte data as single temperature: {e}")
                self.log(f"Raw Data (Hex): {data.hex()}")
        else:
            self.log(f"Invalid Temperature data length: {len(data)} bytes (expected 4 or 8) from {device_display}")
            self.log(f"Raw Data (Hex): {data.hex()}")

    def handle_system_config(self, data, device_display):
        """Handle 19-byte System config with prefix 0x07"""
        if len(data) == 19:
            try:
                # Sys_Config: uint16_t Interrupt_interval, bool overUSB, uint32_t crc32, char reserved[4], uint8_t livedata, uint8_t is_Sensor, uint8_t indolence, bool is_Shipment, char magic[4]
                # Format: '<HBI4sBBBB4s' (uint16, bool, uint32, char[4], uint8, uint8, uint8, bool, char[4]) = 2 + 1 + 4 + 4 + 1 + 1 + 1 + 1 + 4 = 19 bytes
                interrupt_interval, over_usb, crc32, reserved, livedata, is_sensor, indolence, is_shipment, magic = struct.unpack('<HBI4sBBBB4s', data)
                
                # Decode strings
                reserved_str = reserved.decode('utf-8', errors='ignore').rstrip('\x00')
                magic_str = magic.decode('utf-8', errors='ignore').rstrip('\x00')
                
                # Also log to main window
                sys_config_text = f"System Configuration from {device_display}:\n"
                sys_config_text += f"Interrupt Interval: {interrupt_interval} ms\n"
                sys_config_text += f"Over USB: {'Yes' if over_usb else 'No'}\n"
                sys_config_text += f"Live Streaming: {livedata}\n"
                sys_config_text += f"Sensor Enabled: {'Yes' if is_sensor else 'No'}\n"
                sys_config_text += f"Indolence Timeout: {indolence} minutes\n"
                sys_config_text += f"Shipment Mode: {'Yes' if is_shipment else 'No'}\n"
                sys_config_text += f"CRC32: 0x{crc32:08x}\n"
                sys_config_text += f"Reserved: {reserved_str}\n"
                sys_config_text += f"Magic: {magic_str}\n"
                
                self.log(sys_config_text)
            
            except struct.error as e:
                # If parsing fails, just print the hex data
                self.log(f"Failed to parse 19-byte data as Sys_Config: {e}")
                self.log(f"Raw Data (Hex): {data.hex()}")
        else:
            self.log(f"Invalid System config length: {len(data)} bytes (expected 19) from {device_display}")

    def handle_firmware_data(self, data, device_display, address):
        """Handle firmware version data with prefix 0x08"""
        firmware_version_str = None
        
        if len(data) >= 31:  # Expected size for fw_version_t structure (3 + 16 + 12 = 31 bytes)
            try:
                # fw_version_t structure: uint8_t major, minor, patch (3 bytes) + char version[16] + char build_date[12]
                # Format: '<BBB16s12s' (uint8, uint8, uint8, char[16], char[12])
                major, minor, patch, version_bytes, build_date_bytes = struct.unpack('<BBB16s12s', data)
                
                # Decode strings
                version_str = version_bytes.decode('utf-8', errors='ignore').rstrip('\x00')
                build_date_str = build_date_bytes.decode('utf-8', errors='ignore').rstrip('\x00')
                
                # Store firmware version for display (format: vmajor.minor.patch - version number only)
                firmware_version_str = f"v{major}.{minor}.{patch}"
                
                self.log(f"📱 Firmware from {device_display}: {firmware_version_str}, Build: {build_date_str}")
                
            except struct.error as e:
                # Fallback: display as raw text
                try:
                    firmware_text = data.decode('utf-8', errors='ignore').rstrip('\x00')
                    # Extract version number from text (look for pattern like v1.2.3 or 1.2.3)
                    firmware_version_str = self.extract_version_number(firmware_text)
                    self.log(f"📱 Firmware from {device_display}: {firmware_text}")
                except:
                    self.log(f"❌ Error parsing Firmware data from {device_display}: {e}")
        elif len(data) > 0:
            # Handle as text data if not the expected structure size
            try:
                firmware_text = data.decode('utf-8', errors='ignore').rstrip('\x00')
                # Extract version number from text (look for pattern like v1.2.3 or 1.2.3)
                firmware_version_str = self.extract_version_number(firmware_text)
                self.log(f"📱 Firmware from {device_display}: {firmware_text}")
            except:
                self.log(f"❌ Error parsing Firmware data from {device_display}: Invalid data")
        else:
            self.log(f"⚠️  Invalid Firmware data length from {device_display}: got {len(data)} bytes")
        
        # Update device statistics with firmware version
        if firmware_version_str:
            if address not in self.device_stats:
                self.device_stats[address] = {
                    'total_pulses': 0,
                    'total_charge': 0,
                    'battery_voltage': None,
                    'ambient_temperature': None,
                    'firmware_version': None
                }
            self.device_stats[address]['firmware_version'] = firmware_version_str
            
            # Update statistics display to show new firmware version
            self.root.after(0, self.update_stats_display)
    
    def extract_version_number(self, text):
        """Extract version number from text string (e.g., 'II2.3.7' -> 'v2.3.7')"""
        if not text:
            return None
        
        # Try to find version pattern: numbers separated by dots (e.g., 2.3.7, v2.3.7, II2.3.7)
        # Pattern matches: optional prefix, then digits.digits.digits
        match = re.search(r'(\d+\.\d+\.\d+)', text)
        if match:
            version_num = match.group(1)
            return f"v{version_num}"
        
        # If no pattern found, return None
        return None

    def handle_text_response(self, data, device_display):
        """Handle text response with prefix 0x09"""
        try:
            # Decode as text
            text_data = data.decode('utf-8', errors='ignore').strip()
            if text_data:
                self.log(f"Text Response from {device_display}: {text_data}\n\n")
            else:
                self.log(f"Empty text response from {device_display}\n")
        except:
            # If decoding fails, show as hex
            self.log(f"Text Response (hex) from {device_display}: {data.hex()}\n\n")

    def clear_console(self):
        self.console.config(state=tk.NORMAL)
        self.console.delete(1.0, tk.END)
        self.console.config(state=tk.NORMAL)
        self.data_buffer.clear()
        # Clear stats and sensor data records as well
        self.device_stats.clear()
        self.sensor_data_records.clear()

    def force_stop_all_notifications(self):
        """Force stop all notifications and clear all device connections"""
        self.log("Force stopping all notifications and clearing connections...")
        
        # Stop status checker
        self.status_checker_running = False
        
        # Stop dialog monitor
        self.stop_dialog_monitor()
        
        # Clear all clients (this will trigger cleanup in the connection loops)
        clients_to_remove = list(self.clients.keys())
        for address in clients_to_remove:
            if address in self.clients:
                del self.clients[address]
        
        # Clear all tracking data
        self.device_stats.clear()
        self.sensor_data_records.clear()
        self.last_data_time.clear()
        self.last_sensor_data_time.clear()
        self.diagnostic_buffer.clear()
        self.diagnostic_buffer_time.clear()
        self.auto_reconnect_info.clear()
        
        
        # Update UI
        self.update_connected_listbox()
        self.log("All notifications stopped and connections cleared")

    def on_closing(self):
        """Handle application shutdown properly"""
        self.log("Application is closing, cleaning up...")
        
        # Force stop all notifications and connections
        self.force_stop_all_notifications()
        
        # Give a moment for cleanup to complete
        self.root.after(1000, self.root.destroy)

    def start_dialog_monitor(self):
        """Start the dialog monitoring system for sensor data timeout detection"""
        if not self.dialog_monitor_running:
            self.dialog_monitor_running = True
            self.monitor_sensor_data_timeout()

    def stop_dialog_monitor(self):
        """Stop the dialog monitoring system"""
        self.dialog_monitor_running = False
        # Clear timeout records and close popup if it exists
        self.timeout_records.clear()
        if self.timeout_popup:
            try:
                if self.timeout_popup.winfo_exists():
                    self.timeout_popup.destroy()
            except:
                pass
            self.timeout_popup = None

    def monitor_sensor_data_timeout(self):
        """Monitor sensor data timeout and show persistent popup with records"""
        if not self.dialog_monitor_running:
            return
        
        current_time = time.time()
        
        # Check each connected device for sensor data timeout
        for device_addr in list(self.clients.keys()):
            try:
                if device_addr in self.last_sensor_data_time:
                    time_since_last_data = current_time - self.last_sensor_data_time[device_addr]
                    
                    # Check if data timeout threshold exceeded
                    if time_since_last_data > self.data_timeout_threshold:
                        device_name = self.clients[device_addr]['name'] if device_addr in self.clients else f"MAC:{device_addr}"
                        
                        # Create timeout record
                        timeout_timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                        timeout_record = f"DATA_TIMEOUT: {device_name} - No data for {time_since_last_data:.1f}s at {timeout_timestamp}"
                        
                        # Add to records list
                        self.timeout_records.append(timeout_record)
                        
                        # Show/update persistent popup with all records
                        self.show_timeout_popup()
                        
                        # Reset the last data time to prevent continuous popups
                        self.last_sensor_data_time[device_addr] = current_time
                        
            except Exception as e:
                # Silent error handling for dialog monitoring
                continue
        
        # Schedule next check in 300ms (every 300ms as requested)
        if self.dialog_monitor_running:
            self.root.after(300, self.monitor_sensor_data_timeout)

    def show_timeout_popup(self):
        """Show a persistent popup window with all timeout records"""
        try:
            # Create popup if it doesn't exist
            if not self.timeout_popup or not self.timeout_popup.winfo_exists():
                self.timeout_popup = tk.Toplevel(self.root)
                self.timeout_popup.title("Data Timeout Records")
                self.timeout_popup.geometry("800x400")
                self.set_popup_icon(self.timeout_popup)
                self.timeout_popup.configure(bg="white")
                
                # Center the popup
                self.timeout_popup.update_idletasks()
                x = (self.timeout_popup.winfo_screenwidth() // 2) - (800 // 2)
                y = (self.timeout_popup.winfo_screenheight() // 2) - (400 // 2)
                self.timeout_popup.geometry(f"800x400+{x}+{y}")
                
                # Make popup stay on top
                self.timeout_popup.attributes('-topmost', True)
                
                # Create scrollable text widget
                self.timeout_text = tk.Text(
                    self.timeout_popup,
                    font=("Courier", 10),
                    bg="black",
                    fg="red",
                    wrap=tk.WORD,
                    state=tk.DISABLED
                )
                
                # Create scrollbar
                scrollbar = tk.Scrollbar(self.timeout_popup, orient="vertical", command=self.timeout_text.yview)
                self.timeout_text.configure(yscrollcommand=scrollbar.set)
                
                # Pack widgets
                self.timeout_text.pack(side="left", fill="both", expand=True, padx=5, pady=5)
                scrollbar.pack(side="right", fill="y")
                
                # Add close button
                close_btn = tk.Button(
                    self.timeout_popup,
                    text="Clear Records",
                    command=self.clear_timeout_records,
                    bg="red",
                    fg="white",
                    font=("Arial", 10, "bold")
                )
                close_btn.pack(pady=5)
            
            # Update the text content with all records
            if hasattr(self, 'timeout_text'):
                self.timeout_text.config(state=tk.NORMAL)
                self.timeout_text.delete(1.0, tk.END)
                
                # Add header
                self.timeout_text.insert(tk.END, "=== DATA TIMEOUT RECORDS ===\n")
                self.timeout_text.insert(tk.END, f"Total Records: {len(self.timeout_records)}\n")
                self.timeout_text.insert(tk.END, "=" * 50 + "\n\n")
                
                # Check if user is at the bottom before adding new records
                is_at_bottom = self.timeout_text.yview()[1] >= 0.99
                
                # Add all timeout records
                for i, record in enumerate(self.timeout_records, 1):
                    self.timeout_text.insert(tk.END, f"{i:3d}. {record}\n")
                
                self.timeout_text.config(state=tk.DISABLED)
                
                # Only auto-scroll if user was already at the bottom
                if is_at_bottom:
                    self.timeout_text.see(tk.END)
            
        except Exception as e:
            # Silent error handling for popup display
            pass

    def clear_timeout_records(self):
        """Clear all timeout records and close popup"""
        try:
            self.timeout_records.clear()
            if self.timeout_popup and self.timeout_popup.winfo_exists():
                self.timeout_popup.destroy()
            self.timeout_popup = None
        except:
            pass

    def toggle_dialog_monitoring(self):
        """Toggle the dialog monitoring system between start and stop"""
        if self.dialog_monitor_running:
            # Currently running, so stop it
            self.stop_dialog_monitor()
            self.dialog_toggle_btn.config(text="Start Dialogs", bg="lightgreen")
        else:
            # Currently stopped, so start it
            self.start_dialog_monitor()
            self.dialog_toggle_btn.config(text="Stop Dialogs", bg="lightcoral")

    def test_data_parsing(self):
        """Test function to verify data parsing is working correctly"""
        self.log("=== Testing Data Parsing ===")
        
        # Test with sample 20-byte data that matches your Messaging_SensorData struct
        test_data_20 = b'\x01\x00\x00\x00' + b'\xE8\x03' + b'\x0A\x00' + b'\x02\x00' + b'\x64\x00' + b'\x32\x00' + b'\x64\x00' + b'\x0A\x00' + b'\x0B\x00'
        
        self.log(f"Test data (20 bytes): {test_data_20.hex()}")
        
        try:
            timestamp_s, timestamp_ms, pulse_count, charge_count, min_pulse, max_pulse, adc_value, pulseMean_scaled, std_Deviation_scaled = struct.unpack('<IHHHHHHHH', test_data_20)
            formatted_timestamp = f"{timestamp_s}.{timestamp_ms:03d}"
            
            # Convert scaled values back to float
            pulseMean = pulseMean_scaled / 100.0
            std_Deviation = std_Deviation_scaled / 100.0
            
            self.log(f"Parsed successfully:")
            self.log(f"  Timestamp: {formatted_timestamp}")
            self.log(f"  Pulse Count: {pulse_count}")
            self.log(f"  Charge Count: {charge_count}")
            self.log(f"  Min Pulse: {min_pulse}")
            self.log(f"  Max Pulse: {max_pulse}")
            self.log(f"  ADC Value: {adc_value}")
            self.log(f"  Pulse Mean: {pulseMean:.2f}")
            self.log(f"  Std Deviation: {std_Deviation:.2f}")
            self.log("20-byte data parsing is working correctly!")
            
        except struct.error as e:
            self.log(f"20-byte data parsing failed: {e}")
        
        # Test with sample 24-byte data (20 bytes sensor data + 4 bytes additional)
        test_data_24 = b'\x01\x00\x00\x00' + b'\xE8\x03' + b'\x0A\x00' + b'\x02\x00' + b'\x64\x00' + b'\x32\x00' + b'\x64\x00' + b'\x0A\x00' + b'\x0B\x00' + b'\xAA\xBB\xCC\xDD'
        
        self.log(f"Test data (24 bytes): {test_data_24.hex()}")
        
        try:
            sensor_data = test_data_24[:20]
            additional_data = test_data_24[20:]
            
            timestamp_s, timestamp_ms, pulse_count, charge_count, min_pulse, max_pulse, adc_value, pulseMean_scaled, std_Deviation_scaled = struct.unpack('<IHHHHHHHH', sensor_data)
            formatted_timestamp = f"{timestamp_s}.{timestamp_ms:03d}"
            
            # Convert scaled values back to float
            pulseMean = pulseMean_scaled / 100.0
            std_Deviation = std_Deviation_scaled / 100.0
            
            self.log(f"Parsed successfully:")
            self.log(f"  Timestamp: {formatted_timestamp}")
            self.log(f"  Pulse Count: {pulse_count}")
            self.log(f"  Charge Count: {charge_count}")
            self.log(f"  Min Pulse: {min_pulse}")
            self.log(f"  Max Pulse: {max_pulse}")
            self.log(f"  ADC Value: {adc_value}")
            self.log(f"  Pulse Mean: {pulseMean:.2f}")
            self.log(f"  Std Deviation: {std_Deviation:.2f}")
            self.log(f"  Additional Data: {additional_data.hex()}")
            self.log("24-byte data parsing is working correctly!")
            
        except struct.error as e:
            self.log(f"24-byte data parsing failed: {e}")
        
        self.log("=== End Test ===")

    def simulate_20byte_data(self):
        """Simulate receiving 20-byte data to test parsing"""
        self.log("=== Simulating 20-byte Data Reception ===")
        
        # Create sample 20-byte data that matches the STM32 Messaging_SensorData struct
        # timestamp_s=1234567890, timestamp_ms=500, pulse_count=100, charge_count=50, 
        # min_pulse=10, max_pulse=200, adc_value=1500, pulseMean_scaled=12500 (125.00), std_Deviation_scaled=2500 (25.00)
        
        sample_data = struct.pack('<IHHHHHHHH', 
                                 1234567890,  # timestamp_s (uint32)
                                 500,         # timestamp_ms (uint16)
                                 100,         # pulse_count (uint16)
                                 50,          # charge_count (uint16)
                                 10,          # min_pulse (uint16)
                                 200,         # max_pulse (uint16)
                                 1500,        # adc_value (uint16)
                                 12500,       # pulseMean_scaled (uint16) - 125.00 * 100
                                 2500)        # std_Deviation_scaled (uint16) - 25.00 * 100
        
        self.log(f"Simulated data: {sample_data.hex()}")
        
        # Simulate receiving this data from a mock device
        mock_address = "AA:BB:CC:DD:EE:FF"
        mock_device_name = "CB100-TEST"
        
        # Add mock device to clients if not exists
        if mock_address not in self.clients:
            self.clients[mock_address] = {
                'name': mock_device_name,
                'client': None,
                'notify_char_uuid': None
            }
        
        # Call the notification handler with simulated data
        self.notification_handler(None, sample_data, mock_address)
        
        self.log("=== End Simulation ===")

    def toggle_status_checker(self):
        """Toggle the status checker on/off"""
        if not self.status_checker_running:
            self.start_status_checker()
            self.status_checker_btn.config(text="Stop Status Check")
        else:
            self.stop_status_checker()
            self.status_checker_btn.config(text="Start Status Check")
    
    def start_status_checker(self):
        """Start the status checker to monitor data reception"""
        if not self.status_checker_running:
            self.status_checker_running = True
            self.check_data_status()
    
    def stop_status_checker(self):
        """Stop the status checker"""
        self.status_checker_running = False
        self.log("Status checker stopped")
    
    def check_data_status(self):
        """Check the status of data reception for all connected devices"""
        if not self.status_checker_running:
            return
        
        current_time = time.time()
        # Create copies to avoid modification during iteration
        clients_copy = dict(self.clients)
        last_data_time_copy = dict(self.last_data_time)
        
        for address in list(clients_copy.keys()):
            try:
                if address in last_data_time_copy:
                    time_since_last_data = current_time - last_data_time_copy[address]
                    if time_since_last_data > 10:  # More than 10 seconds without data
                        device_name = clients_copy[address]['name'] if address in clients_copy else "Unknown"
                        if device_name == address:  # MAC-only device
                            device_display = f"MAC:{address}"
                        else:
                            device_display = f"{device_name} [{address}]"
                        
                        self.log(f"WARNING: {device_display} - No data received for {time_since_last_data:.1f} seconds")
            except Exception as e:
                # Log error but continue with other devices
                self.log(f"Error checking data status for {address}: {e}")
                continue
        
        # Schedule next check in 5 seconds
        if self.status_checker_running:
            self.root.after(5000, self.check_data_status)

    def request_data_from_device(self):
        """Check data reception status from the connected device."""
        if not self.clients:
            messagebox.showinfo("Info", "No device connected.")
            return

        selected_indices = self.connected_listbox.curselection()
        if not selected_indices:
            messagebox.showinfo("Info", "Please select a device to check.")
            return

        selected_device_addr = self.listbox_addr_map[selected_indices[0]]
        device_name = self.clients[selected_device_addr]['name']

        # Show MAC address prominently for MAC-only devices
        if device_name == selected_device_addr:  # MAC-only device
            device_display = f"MAC:{selected_device_addr}"
        else:
            device_display = f"{device_name} [{selected_device_addr}]"

        self.log(f"Checking data reception status for {device_display}...")

        async def check_data_status_async():
            try:
                client = self.clients[selected_device_addr]['client']
                notify_char_uuid = self.clients[selected_device_addr]['notify_char_uuid']
                write_char_uuid = self.clients[selected_device_addr].get('write_char_uuid')

                # Check connection status
                is_connected = False
                if hasattr(client, 'is_connected'):
                    if callable(client.is_connected):
                        is_connected = client.is_connected()
                    else:
                        is_connected = client.is_connected
                
                if not is_connected:
                    self.log(f"ERROR: Device {device_display} is not connected.")
                    return

                # Check if notifications are set up
                if not notify_char_uuid:
                    self.log(f"WARNING: No notify characteristic found for {device_display}.")
                    self.log(f"Device may not be sending data automatically.")
                    return

                self.log(f"Device {device_display} is connected and notifications are active.")
                self.log(f"Notify characteristic: {notify_char_uuid}")
                
                if write_char_uuid:
                    self.log(f"Write characteristic available: {write_char_uuid}")
                else:
                    self.log(f"No write characteristic available (read-only device)")

                # Check last data reception time
                if selected_device_addr in self.last_data_time:
                    time_since_last_data = time.time() - self.last_data_time[selected_device_addr]
                    self.log(f"Last data received: {time_since_last_data:.1f} seconds ago")
                    
                    if time_since_last_data > 10:
                        self.log(f"WARNING: No data received for {time_since_last_data:.1f} seconds")
                        self.log(f"Device may not be sending data or there may be a connection issue")
                    else:
                        self.log(f"Data reception is active and recent")
                else:
                    self.log(f"No data received yet from this device")
                    self.log(f"Device should start sending data automatically")

                # Check device statistics
                if selected_device_addr in self.device_stats:
                    stats = self.device_stats[selected_device_addr]
                    self.log(f"Data packets received: {stats['packet_count']}")
                    self.log(f"Total pulses: {stats['total_pulses']}")
                    self.log(f"Total charge: {stats['total_charge']}")
                else:
                    self.log(f"No statistics available yet")

            except Exception as e:
                self.log(f"Error checking data status for {device_display}: {e}")

        threading.Thread(target=lambda: asyncio.run(check_data_status_async()), daemon=True).start()

    def send_custom_command(self):
        """Send a custom command to the connected device"""
        if not self.clients:
            messagebox.showinfo("Info", "No device connected to send commands to.")
            return

        selected_indices = self.connected_listbox.curselection()
        if not selected_indices:
            messagebox.showinfo("Info", "Please select a device to send commands to.")
            return

        selected_device_addr = self.listbox_addr_map[selected_indices[0]]
        device_name = self.clients[selected_device_addr]['name']

        # Show MAC address prominently for MAC-only devices
        if device_name == selected_device_addr:  # MAC-only device
            device_display = f"MAC:{selected_device_addr}"
        else:
            device_display = f"{device_name} [{selected_device_addr}]"

        # Create command input dialog
        popup = tk.Toplevel(self.root)
        popup.title("Send Custom Command")
        popup.geometry("400x300")
        self.set_popup_icon(popup)
        popup.update()
        center_window(popup, 400, 300)
        
        tk.Label(popup, text=f"Send command to {device_display}").pack(pady=10)
        
        # Command type selection
        tk.Label(popup, text="Command Type:").pack(pady=5)
        command_type = tk.StringVar(value="text")
        # tk.Radiobutton(popup, text="Hex (e.g., 01, FF, 1234)", variable=command_type, value="hex").pack()
        # tk. Radiobutton(popup, text="Text (e.g., START, DATA)", variable=command_type, value="text").pack()
        tk.Label(popup, text="Text (e.g., set, diagnose, flash_info, ram_info, temp amb, temp mcu, erase, reset)").pack()
        # tk.Radiobutton(popup, text="Single Byte (0-255)", variable=command_type, value="byte").pack()
        
        # Command input
        tk.Label(popup, text="Command:").pack(pady=5)
        command_entry = tk.Entry(popup, width=20, font=('Courier', 12))
        command_entry.pack(pady=5)
        command_entry.insert(0, "batt")
        command_entry.focus()
        
        # Status label
        status_label = tk.Label(popup, text="", fg="blue")
        status_label.pack(pady=5)
        
        def send_command():
            try:
                cmd_text = command_entry.get().strip()
                cmd_type = command_type.get()
                
                if not cmd_text:
                    status_label.config(text="Please enter a command", fg="red")
                    return
                
                # Convert command based on type
                if cmd_type == "hex":
                    # Remove spaces and convert hex string to bytes
                    hex_clean = cmd_text.replace(" ", "").replace("0x", "").replace("0X", "")
                    if len(hex_clean) % 2 != 0:
                        status_label.config(text="Hex string must have even length", fg="red")
                        return
                    cmd_bytes = bytes.fromhex(hex_clean)
                elif cmd_type == "text":
                    cmd_bytes = cmd_text.encode('utf-8')
                elif cmd_type == "byte":
                    try:
                        byte_val = int(cmd_text)
                        if byte_val < 0 or byte_val > 255:
                            status_label.config(text="Byte value must be 0-255", fg="red")
                            return
                        cmd_bytes = bytes([byte_val])
                    except ValueError:
                        status_label.config(text="Invalid byte value", fg="red")
                        return
                
                status_label.config(text="Sending command...", fg="blue")
                popup.update()
                
                # Send the command
                async def send_async():
                    try:
                        client = self.clients[selected_device_addr]['client']
                        write_char_uuid = self.clients[selected_device_addr].get('write_char_uuid')
                        
                        if not write_char_uuid:
                            self.log(f"ERROR: No write characteristic found for {device_display}")
                            status_label.config(text="No write characteristic available", fg="red")
                            return
                        
                        await client.write_gatt_char(write_char_uuid, cmd_bytes)
                        # self.log(f"Sent command to {device_display}: {cmd_bytes.hex()} ({cmd_text})")
                        self.log(f"Sent command to {device_display}: ({cmd_text})")
                        status_label.config(text="Command sent successfully!", fg="green")
                        
                        # Close popup after 2 seconds
                        # popup.after(2000, popup.destroy)
                        
                    except Exception as e:
                        self.log(f"Error sending command to {device_display}: {e}")
                        status_label.config(text=f"Error: {e}", fg="red")
                
                threading.Thread(target=lambda: asyncio.run(send_async()), daemon=True).start()
                
            except Exception as e:
                status_label.config(text=f"Error: {e}", fg="red")
        
        # Buttons
        button_frame = tk.Frame(popup)
        button_frame.pack(pady=10)
        
        send_btn = tk.Button(button_frame, text="Send", command=send_command)
        send_btn.pack(side=tk.LEFT, padx=5)
        
        cancel_btn = tk.Button(button_frame, text="Cancel", command=popup.destroy)
        cancel_btn.pack(side=tk.LEFT, padx=5)
        
        # Bind Enter key
        command_entry.bind('<Return>', lambda e: send_command())

    def start_auto_reconnect(self, address):
        """Start auto-reconnect process for a device"""
        if address not in self.auto_reconnect_info:
            return
            
        reconnect_info = self.auto_reconnect_info[address]
        if not reconnect_info['auto_reconnect']:
            return
            
        # Increment reconnect attempts
        reconnect_info['reconnect_attempts'] += 1
        
        if reconnect_info['reconnect_attempts'] > reconnect_info['max_reconnect_attempts']:
            self.log(f"Auto-reconnect failed after {reconnect_info['max_reconnect_attempts']} attempts for {address}")
            # Remove from auto-reconnect tracking
            del self.auto_reconnect_info[address]
            return
            
        device = reconnect_info['original_device']
        device_name = device.name if device.name else f"MAC:{address}"
        
        self.log(f"Auto-reconnect attempt {reconnect_info['reconnect_attempts']}/{reconnect_info['max_reconnect_attempts']} for {device_name}")
        
        # Wait before attempting reconnection (exponential backoff)
        delay = min(5 * reconnect_info['reconnect_attempts'], 30)  # Max 30 seconds
        self.root.after(delay * 1000, lambda: self.attempt_reconnect(address))

    def attempt_reconnect(self, address):
        """Attempt to reconnect to a device"""
        if address not in self.auto_reconnect_info:
            return
            
        reconnect_info = self.auto_reconnect_info[address]
        if not reconnect_info['auto_reconnect']:
            return
            
        device = reconnect_info['original_device']
        device_name = device.name if device.name else f"MAC:{address}"
        
        # Check if device is already connected
        if address in self.clients:
            self.log(f"Device {device_name} is already connected, stopping auto-reconnect")
            del self.auto_reconnect_info[address]
            return
            
        self.log(f"Attempting to reconnect to {device_name}...")
        
        # Start connection in a separate thread
        # connect_to_device is already a regular function that handles threading internally
        threading.Thread(target=lambda: self.connect_to_device(device), daemon=True).start()

    def toggle_auto_reconnect(self):
        """Toggle auto-reconnect for the selected device"""
        if not self.clients:
            messagebox.showinfo("Info", "No device connected.")
            return

        selected_indices = self.connected_listbox.curselection()
        if not selected_indices:
            messagebox.showinfo("Info", "Please select a device to toggle auto-reconnect.")
            return

        selected_device_addr = self.listbox_addr_map[selected_indices[0]]
        device_name = self.clients[selected_device_addr]['name']

        # Show MAC address prominently for MAC-only devices
        if device_name == selected_device_addr:  # MAC-only device
            device_display = f"MAC:{selected_device_addr}"
        else:
            device_display = f"{device_name} [{selected_device_addr}]"

        # Initialize auto-reconnect info if not exists
        if selected_device_addr not in self.auto_reconnect_info:
            self.auto_reconnect_info[selected_device_addr] = {
                'auto_reconnect': False,
                'original_device': None,
                'reconnect_attempts': 0,
                'max_reconnect_attempts': 10
            }
            
            # Create a mock device object for reconnection
            class MockDevice:
                def __init__(self, address, name):
                    self.address = address
                    self.name = name
            
            self.auto_reconnect_info[selected_device_addr]['original_device'] = MockDevice(
                selected_device_addr, device_name
            )

        # Toggle auto-reconnect
        current_state = self.auto_reconnect_info[selected_device_addr]['auto_reconnect']
        self.auto_reconnect_info[selected_device_addr]['auto_reconnect'] = not current_state
        
        new_state = self.auto_reconnect_info[selected_device_addr]['auto_reconnect']
        
        if new_state:
            self.log(f"Auto-reconnect ENABLED for {device_display}")
            messagebox.showinfo("Auto-Reconnect", f"Auto-reconnect enabled for {device_display}")
        else:
            self.log(f"Auto-reconnect DISABLED for {device_display}")
            messagebox.showinfo("Auto-Reconnect", f"Auto-reconnect disabled for {device_display}")

    def show_auto_reconnect_status(self):
        """Show the auto-reconnect status for all devices"""
        if not self.auto_reconnect_info:
            messagebox.showinfo("Auto-Reconnect Status", "No auto-reconnect information available.")
            return
        
        status_text = "Auto-Reconnect Status:\n\n"
        
        for addr, info in self.auto_reconnect_info.items():
            device_name = "Unknown"
            if addr in self.clients:
                device_name = self.clients[addr]['name']
            elif info['original_device']:
                device_name = info['original_device'].name or f"MAC:{addr}"
            
            # Show MAC address prominently for MAC-only devices
            if device_name == addr:  # MAC-only device
                device_display = f"MAC:{addr}"
            else:
                device_display = f"{device_name} [{addr}]"
            
            status = "ENABLED" if info['auto_reconnect'] else "DISABLED"
            attempts = info['reconnect_attempts']
            max_attempts = info['max_reconnect_attempts']
            
            status_text += f"{device_display}:\n"
            status_text += f"  Status: {status}\n"
            status_text += f"  Attempts: {attempts}/{max_attempts}\n"
            status_text += f"  Connected: {'Yes' if addr in self.clients else 'No'}\n\n"
        
        messagebox.showinfo("Auto-Reconnect Status", status_text)

    def show_api_help(self):
        """Show API command list in a popup window"""
        popup = tk.Toplevel(self.root)
        popup.title("CB100 API Commands")
        popup.geometry("550x600")
        self.set_popup_icon(popup)
        popup.update()
        center_window(popup, 550, 600)
        
        # Create scrollable text widget
        text_frame = tk.Frame(popup)
        text_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        scrollbar = tk.Scrollbar(text_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        api_text = tk.Text(text_frame, width=65, height=30, font=("Consolas", 10), 
                           wrap=tk.WORD, yscrollcommand=scrollbar.set, state=tk.NORMAL)
        api_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=api_text.yview)
        
        # API list content from main.c
        api_content = """LIST OF APIs:

set
  Ex: set Oct 20 2025 11:25:44
  Synchronize device date/time

diagnose
  Display Diagnose Info
  Returns comprehensive system diagnostic information

flash_info
  Display Flash Info
  Shows NAND flash memory specifications and status

ram_info
  Display RAM Info
  Shows current RAM usage statistics

read
  Read the data from flash

erase
  Erase Flash (only data region)
  WARNING: This will erase data region!

reset
  Reload the system settings to default

reboot
  Reboot the system

temp
  Returns Ambient & MCU temperature

temp amb
  Returns Ambient temperature only

temp mcu
  Returns MCU temperature only

temp set
  Stores Ambient & MCU temperature into flash

sysconfig
  Returns System Configuration

sysconfig 0
  Switch to BLE Mode

sysconfig 1
  Switch to USB Mode

sysconfig 2
  Turns Sensor data telemetry OFF

sysconfig 3
  Turns Sensor data telemetry ON

sysconfig 4
  Turns the Sensor OFF

sysconfig 5
  Turns the Sensor ON

sysconfig 100-4000
  Set the time interval (in milliseconds)

00x01
  Enables the Shipping Mode

00x00
  Disables the Shipping Mode (Over USB only)

batt
  Returns Battery voltage and ADC Values

firmware
  Returns firmware Version (e.g., v2.3.7 - Comments are included)

sleepin
  Set time in minutes (1-255mins) the Sleep Time Out, 0 means no sleep

ischarging
  Returns Charging Status

help
  Returns Help Info"""
        
        api_text.insert("1.0", api_content)
        api_text.config(state=tk.DISABLED)
        
        # Close button
        close_btn = tk.Button(popup, text="Close", command=popup.destroy, width=15)
        close_btn.pack(pady=10)
    
    def show_ubuntu_help(self):
        """Show Ubuntu-specific help and troubleshooting information"""
        help_text = """Ubuntu BLE Troubleshooting Guide

SYSTEM REQUIREMENTS:
• Ubuntu 20.04+ recommended
• Python 3.8+ with bleak library
• BlueZ Bluetooth stack

INSTALLATION:
1. Install required packages:
   sudo apt update
   sudo apt install bluez bluez-tools python3-pip

2. Install Python dependencies:
   pip3 install bleak tkinter

3. Add user to bluetooth group:
   sudo usermod -a -G bluetooth $USER
   (Log out and back in)

4. Start Bluetooth service:
   sudo systemctl start bluetooth
   sudo systemctl enable bluetooth

TROUBLESHOOTING:
• If devices not found: Check system check results above
• If connection fails: Try 'ble_linux' command on STM32 device
• If permission denied: Ensure user is in bluetooth group
• If service not running: sudo systemctl restart bluetooth

COMMAND LINE TESTING:
• Scan for devices: sudo hcitool lescan
• Check adapter: sudo hciconfig
• Bluetooth control: bluetoothctl

OPTIMIZATIONS ENABLED:
• Extended scan timeouts (12s vs 8s)
• Enhanced connection retry logic (5 retries vs 3)
• Longer connection timeouts (20s vs 15s)
• Improved service discovery error handling
• Ubuntu-specific device filtering

For more help, check the system check results above."""
        
        messagebox.showinfo("Ubuntu BLE Help", help_text)

    def quick_connect(self):
        """Quick connect to recently discovered devices with Ubuntu optimizations"""
        import platform
        
        if not hasattr(self, 'previous_scan_results') or not self.previous_scan_results:
            messagebox.showinfo("Info", "No recent scan results available. Please scan for devices first.")
            return
        
        is_linux = platform.system().lower() == 'linux'
        
        # Create popup for quick connect
        popup = tk.Toplevel(self.root)
        popup.title("Quick Connect - Recent Devices" + (" (Ubuntu Optimized)" if is_linux else ""))
        popup.geometry("400x300")
        self.set_popup_icon(popup)
        popup.update()
        center_window(popup, 400, 500)
        
        # Create listbox for recent devices
        listbox = tk.Listbox(popup, width=50, selectmode=tk.SINGLE)
        listbox.pack(pady=10, fill=tk.BOTH, expand=True)
        
        # Add recent CB100 devices to listbox with Ubuntu-specific filtering
        for address, info in self.previous_scan_results.items():
            if info['name'] and (info['name'].startswith('CB100-') or info['name'].startswith('CB00000')):
                # Display only device name, not MAC address
                display_text = info['name']
                if is_linux and 'rssi' in info:
                    display_text += f" (RSSI: {info['rssi']})"
                listbox.insert(tk.END, display_text)
        
        # Count CB100 devices in recent results
        cb100_recent_count = sum(1 for info in self.previous_scan_results.values() 
                                if info['name'] and (info['name'].startswith('CB100-') or info['name'].startswith('CB00000')))
        
        status_text = f"Found {cb100_recent_count} recent CB100 devices"
        if is_linux:
            status_text += " (Ubuntu optimized)"
        status = tk.Label(popup, text=status_text)
        status.pack(pady=5)
        
        def connect_selected():
            idx = listbox.curselection()
            if not idx:
                return
            
            # Get the selected CB100 device address
            cb100_addresses = [addr for addr, info in self.previous_scan_results.items() 
                             if info['name'] and info['name'].startswith('CB100-')]
            selected_address = cb100_addresses[idx[0]]
            device_info = self.previous_scan_results[selected_address]
            
            # Create mock device object
            class MockDevice:
                def __init__(self, address, name):
                    self.address = address
                    self.name = name
            
            mock_device = MockDevice(selected_address, device_info['name'])
            popup.destroy()
            self.connect_to_device(mock_device)
        
        # Add Connect button
        connect_btn = tk.Button(popup, text="Connect", command=connect_selected)
        connect_btn.pack(pady=5)
        
        # Add Cancel button
        cancel_btn = tk.Button(popup, text="Cancel", command=popup.destroy)
        cancel_btn.pack(pady=2)
        
        # Add Force Connect button for stubborn devices
        force_connect_btn = tk.Button(popup, text="Force Connect (Advanced)", command=lambda: self.force_connect_selected(listbox), fg="red")
        force_connect_btn.pack(pady=2)

    def force_connect_selected(self, listbox):
        """Force connect to a selected CB100 device with aggressive retry settings"""
        idx = listbox.curselection()
        if not idx:
            return
        
        # Get the selected CB100 device address
        cb100_addresses = [addr for addr, info in self.previous_scan_results.items() 
                          if info['name'] and info['name'].startswith('CB100-')]
        selected_address = cb100_addresses[idx[0]]
        device_info = self.previous_scan_results[selected_address]
        
        # Create mock device object
        class MockDevice:
            def __init__(self, address, name):
                self.address = address
                self.name = name
        
        mock_device = MockDevice(selected_address, device_info['name'])
        
        # Show warning dialog
        result = messagebox.askyesno(
            "Force Connect", 
            f"Force connect to {device_info['name'] or 'MAC:' + selected_address}?\n\n"
            "This will use aggressive connection settings and may take longer.\n"
            "Continue?"
        )
        
        if result:
            self.log(f"Force connecting to {selected_address}...")
            # Use the same connect method but with more aggressive settings
            self.connect_to_device(mock_device)

    def set_battery_monitor_interval(self):
        """Set the battery monitoring interval and start/stop monitoring"""
        popup = tk.Toplevel(self.root)
        popup.title("Battery Monitor Settings")
        popup.geometry("400x250")
        self.set_popup_icon(popup)
        popup.update()
        center_window(popup, 400, 250)
        
        # Current interval label - display in minutes or hours
        current_interval_min = self.battery_monitoring_interval // 60
        if current_interval_min >= 60:
            current_interval_hr = current_interval_min // 60
            current_display = f"{current_interval_hr} hour{'s' if current_interval_hr > 1 else ''}" if current_interval_min % 60 == 0 else f"{current_interval_hr}h {current_interval_min % 60}min"
        else:
            current_display = f"{current_interval_min} minute{'s' if current_interval_min > 1 else ''}"
        
        status_text = f"Current Interval: {current_display} ({self.battery_monitoring_interval} seconds)\n"
        status_text += f"Monitoring: {'Active' if self.battery_monitoring_active else 'Inactive'}"
        status_label = tk.Label(popup, text=status_text, fg="blue", justify=tk.LEFT)
        status_label.pack(pady=10)
        
        # Interval input - accept minutes (1 min to 720 min = 12 hours)
        tk.Label(popup, text="Monitoring Interval (minutes):").pack(pady=5)
        tk.Label(popup, text="Range: 1 minute to 720 minutes (12 hours)", font=("Arial", 8), fg="gray").pack()
        interval_entry = tk.Entry(popup, width=20, font=('Courier', 12))
        interval_entry.pack(pady=5)
        interval_entry.insert(0, str(self.battery_monitoring_interval // 60))  # Show current value in minutes
        interval_entry.focus()
        
        # Status message label
        msg_label = tk.Label(popup, text="", fg="green")
        msg_label.pack(pady=5)
        
        def apply_settings():
            try:
                interval_minutes = int(interval_entry.get().strip())
                if interval_minutes < 1:
                    msg_label.config(text="Interval must be at least 1 minute", fg="red")
                    return
                if interval_minutes > 720:  # 12 hours = 720 minutes
                    msg_label.config(text="Interval cannot exceed 720 minutes (12 hours)", fg="red")
                    return
                
                # Convert minutes to seconds
                interval_seconds = interval_minutes * 60
                self.battery_monitoring_interval = interval_seconds
                
                # Format interval display
                if interval_minutes >= 60:
                    interval_hr = interval_minutes // 60
                    interval_display = f"{interval_hr}hr" if interval_minutes % 60 == 0 else f"{interval_hr}h{interval_minutes % 60}min"
                else:
                    interval_display = f"{interval_minutes}min"
                
                # Update button text with new interval
                if self.battery_monitoring_active:
                    self.battery_monitor_btn.config(text=f"Battery Monitor ({interval_display})")
                
                # Update status label with formatted interval
                if interval_minutes >= 60:
                    interval_hr = interval_minutes // 60
                    current_display = f"{interval_hr} hour{'s' if interval_hr > 1 else ''}" if interval_minutes % 60 == 0 else f"{interval_hr}h {interval_minutes % 60}min"
                else:
                    current_display = f"{interval_minutes} minute{'s' if interval_minutes > 1 else ''}"
                
                # Restart monitoring if it was active
                was_active = self.battery_monitoring_active
                if was_active:
                    self.stop_battery_monitoring()
                    self.start_battery_monitoring()
                    msg_label.config(text=f"Monitoring restarted with {current_display} interval", fg="green")
                else:
                    msg_label.config(text=f"Interval set to {current_display}. Click 'Start' to begin.", fg="green")
                
                status_label.config(text=f"Current Interval: {current_display} ({interval_seconds} seconds)\nMonitoring: {'Active' if self.battery_monitoring_active else 'Inactive'}")
                
            except ValueError:
                msg_label.config(text="Please enter a valid number (1-720 minutes)", fg="red")
        
        def format_interval_display(interval_seconds):
            """Helper function to format interval for display"""
            interval_minutes = interval_seconds // 60
            if interval_minutes >= 60:
                interval_hr = interval_minutes // 60
                interval_remainder_min = interval_minutes % 60
                if interval_remainder_min == 0:
                    return f"{interval_hr} hour{'s' if interval_hr > 1 else ''}"
                else:
                    return f"{interval_hr}h {interval_remainder_min}min"
            else:
                return f"{interval_minutes} minute{'s' if interval_minutes > 1 else ''}"
        
        def start_monitoring():
            if not self.clients:
                msg_label.config(text="No devices connected. Please connect a device first.", fg="red")
                return
            
            if self.battery_monitoring_active:
                msg_label.config(text="Battery monitoring is already active", fg="orange")
                return
            
            self.start_battery_monitoring()
            current_display = format_interval_display(self.battery_monitoring_interval)
            status_label.config(text=f"Current Interval: {current_display} ({self.battery_monitoring_interval} seconds)\nMonitoring: {'Active' if self.battery_monitoring_active else 'Inactive'}")
            msg_label.config(text=f"Battery monitoring started ({current_display} interval)", fg="green")
        
        def stop_monitoring():
            if not self.battery_monitoring_active:
                msg_label.config(text="Battery monitoring is not active", fg="orange")
                return
            
            self.stop_battery_monitoring()
            current_display = format_interval_display(self.battery_monitoring_interval)
            status_label.config(text=f"Current Interval: {current_display} ({self.battery_monitoring_interval} seconds)\nMonitoring: {'Active' if self.battery_monitoring_active else 'Inactive'}")
            msg_label.config(text="Battery monitoring stopped", fg="green")
        
        # Buttons frame
        button_frame = tk.Frame(popup)
        button_frame.pack(pady=10)
        
        apply_btn = tk.Button(button_frame, text="Apply Interval", command=apply_settings, width=15)
        apply_btn.pack(side=tk.LEFT, padx=5)
        
        start_btn = tk.Button(button_frame, text="Start", command=start_monitoring, width=12, bg="lightgreen")
        start_btn.pack(side=tk.LEFT, padx=5)
        
        stop_btn = tk.Button(button_frame, text="Stop", command=stop_monitoring, width=12, bg="lightcoral")
        stop_btn.pack(side=tk.LEFT, padx=5)
        
        close_btn = tk.Button(button_frame, text="Close", command=popup.destroy, width=12)
        close_btn.pack(side=tk.LEFT, padx=5)
        
        # Bind Enter key to apply
        interval_entry.bind('<Return>', lambda e: apply_settings())
    
    def start_battery_monitoring(self):
        """Start periodic battery voltage monitoring"""
        if self.battery_monitoring_active:
            return  # Already monitoring
        
        if not self.clients:
            self.log("ERROR: Cannot start battery monitoring - no devices connected")
            return
        
        self.battery_monitoring_active = True
        # Format interval display for log and button
        interval_minutes = self.battery_monitoring_interval // 60
        if interval_minutes >= 60:
            # Display in hours
            interval_hr = interval_minutes // 60
            interval_remainder_min = interval_minutes % 60
            if interval_remainder_min == 0:
                interval_display = f"{interval_hr}hr"
                interval_log = f"{interval_hr} hour{'s' if interval_hr > 1 else ''} ({self.battery_monitoring_interval}s)"
            else:
                interval_display = f"{interval_hr}h{interval_remainder_min}min"
                interval_log = f"{interval_hr}h {interval_remainder_min}min ({self.battery_monitoring_interval}s)"
        elif interval_minutes >= 1:
            # Display in minutes
            interval_display = f"{interval_minutes}min"
            interval_log = f"{interval_minutes} minute{'s' if interval_minutes > 1 else ''} ({self.battery_monitoring_interval}s)"
        else:
            # Display in seconds (shouldn't happen with new validation, but keep for safety)
            interval_display = f"{self.battery_monitoring_interval}s"
            interval_log = f"{self.battery_monitoring_interval} seconds"
        
        self.log(f"Battery monitoring started (interval: {interval_log})")
        
        # Update button text
        self.battery_monitor_btn.config(text=f"Battery Monitor ({interval_display})", bg="green")
        
        # Send initial battery request
        self.request_battery_status_all_devices()
        
        # Schedule periodic requests
        self.schedule_next_battery_request()
    
    def stop_battery_monitoring(self):
        """Stop periodic battery voltage monitoring"""
        if not self.battery_monitoring_active:
            return  # Not monitoring
        
        self.battery_monitoring_active = False
        
        # Cancel scheduled job if exists
        if self.battery_monitoring_job:
            self.root.after_cancel(self.battery_monitoring_job)
            self.battery_monitoring_job = None
        
        # Update button text
        self.battery_monitor_btn.config(text="Battery Monitor (OFF)", bg="gray")
        
        self.log("Battery monitoring stopped")
    
    def schedule_next_battery_request(self):
        """Schedule the next battery status request"""
        if not self.battery_monitoring_active:
            return
        
        # Schedule next request
        self.battery_monitoring_job = self.root.after(
            self.battery_monitoring_interval * 1000,  # Convert seconds to milliseconds
            self.request_battery_status_all_devices
        )
    
    def request_battery_status_all_devices(self):
        """Request battery status from all connected devices"""
        if not self.battery_monitoring_active:
            return
        
        if not self.clients:
            self.stop_battery_monitoring()
            return
        
        # Send "batt" command to all connected devices
        clients_copy = dict(self.clients)
        for address, client_info in clients_copy.items():
            try:
                write_char_uuid = client_info.get('write_char_uuid')
                if write_char_uuid:
                    device_name = client_info['name']
                    if device_name == address:
                        device_display = f"MAC:{address}"
                    else:
                        device_display = f"{device_name}"
                    
                    # Send battery command asynchronously
                    async def send_batt_command(addr, uuid, display):
                        try:
                            client = self.clients[addr]['client']
                            if addr in self.clients:  # Double check device still connected
                                await client.write_gatt_char(uuid, b"batt")
                                # Don't log every request to reduce spam
                        except Exception as e:
                            self.log(f"Error requesting battery status from {display}: {e}")
                            # Remove device if connection lost
                            if addr in self.clients:
                                del self.clients[addr]
                                self.update_connected_listbox()
                    
                    threading.Thread(
                        target=lambda: asyncio.run(send_batt_command(address, write_char_uuid, device_display)),
                        daemon=True
                    ).start()
            except Exception as e:
                self.log(f"Error preparing battery request for {address}: {e}")
        
        # Schedule next request
        self.schedule_next_battery_request()
    
    def set_temperature_monitor_interval(self):
        """Set the temperature monitoring interval and start/stop monitoring"""
        popup = tk.Toplevel(self.root)
        popup.title("Temperature Monitor Settings")
        popup.geometry("400x250")
        self.set_popup_icon(popup)
        popup.update()
        center_window(popup, 400, 250)
        
        # Current interval label - display in minutes or hours
        current_interval_min = self.temperature_monitoring_interval // 60
        if current_interval_min >= 60:
            current_interval_hr = current_interval_min // 60
            current_display = f"{current_interval_hr} hour{'s' if current_interval_hr > 1 else ''}" if current_interval_min % 60 == 0 else f"{current_interval_hr}h {current_interval_min % 60}min"
        else:
            current_display = f"{current_interval_min} minute{'s' if current_interval_min > 1 else ''}"
        
        status_text = f"Current Interval: {current_display} ({self.temperature_monitoring_interval} seconds)\n"
        status_text += f"Monitoring: {'Active' if self.temperature_monitoring_active else 'Inactive'}"
        status_label = tk.Label(popup, text=status_text, fg="blue", justify=tk.LEFT)
        status_label.pack(pady=10)
        
        # Interval input - accept minutes (1 min to 720 min = 12 hours)
        tk.Label(popup, text="Monitoring Interval (minutes):").pack(pady=5)
        tk.Label(popup, text="Range: 1 minute to 720 minutes (12 hours)", font=("Arial", 8), fg="gray").pack()
        interval_entry = tk.Entry(popup, width=20, font=('Courier', 12))
        interval_entry.pack(pady=5)
        interval_entry.insert(0, str(self.temperature_monitoring_interval // 60))  # Show current value in minutes
        interval_entry.focus()
        
        # Status message label
        msg_label = tk.Label(popup, text="", fg="green")
        msg_label.pack(pady=5)
        
        def apply_settings():
            try:
                interval_minutes = int(interval_entry.get().strip())
                if interval_minutes < 1:
                    msg_label.config(text="Interval must be at least 1 minute", fg="red")
                    return
                if interval_minutes > 720:  # 12 hours = 720 minutes
                    msg_label.config(text="Interval cannot exceed 720 minutes (12 hours)", fg="red")
                    return
                
                # Convert minutes to seconds
                interval_seconds = interval_minutes * 60
                self.temperature_monitoring_interval = interval_seconds
                
                # Format interval display
                if interval_minutes >= 60:
                    interval_hr = interval_minutes // 60
                    interval_display = f"{interval_hr}hr" if interval_minutes % 60 == 0 else f"{interval_hr}h{interval_minutes % 60}min"
                else:
                    interval_display = f"{interval_minutes}min"
                
                # Update button text with new interval
                if self.temperature_monitoring_active:
                    self.temperature_monitor_btn.config(text=f"Temp Monitor ({interval_display})")
                
                # Update status label with formatted interval
                if interval_minutes >= 60:
                    interval_hr = interval_minutes // 60
                    current_display = f"{interval_hr} hour{'s' if interval_hr > 1 else ''}" if interval_minutes % 60 == 0 else f"{interval_hr}h {interval_minutes % 60}min"
                else:
                    current_display = f"{interval_minutes} minute{'s' if interval_minutes > 1 else ''}"
                
                # Restart monitoring if it was active
                was_active = self.temperature_monitoring_active
                if was_active:
                    self.stop_temperature_monitoring()
                    self.start_temperature_monitoring()
                    msg_label.config(text=f"Monitoring restarted with {current_display} interval", fg="green")
                else:
                    msg_label.config(text=f"Interval set to {current_display}. Click 'Start' to begin.", fg="green")
                
                status_label.config(text=f"Current Interval: {current_display} ({interval_seconds} seconds)\nMonitoring: {'Active' if self.temperature_monitoring_active else 'Inactive'}")
                
            except ValueError:
                msg_label.config(text="Please enter a valid number (1-720 minutes)", fg="red")
        
        def format_interval_display(interval_seconds):
            """Helper function to format interval for display"""
            interval_minutes = interval_seconds // 60
            if interval_minutes >= 60:
                interval_hr = interval_minutes // 60
                interval_remainder_min = interval_minutes % 60
                if interval_remainder_min == 0:
                    return f"{interval_hr} hour{'s' if interval_hr > 1 else ''}"
                else:
                    return f"{interval_hr}h {interval_remainder_min}min"
            else:
                return f"{interval_minutes} minute{'s' if interval_minutes > 1 else ''}"
        
        def start_monitoring():
            if not self.clients:
                msg_label.config(text="No devices connected. Please connect a device first.", fg="red")
                return
            
            if self.temperature_monitoring_active:
                msg_label.config(text="Temperature monitoring is already active", fg="orange")
                return
            
            self.start_temperature_monitoring()
            current_display = format_interval_display(self.temperature_monitoring_interval)
            status_label.config(text=f"Current Interval: {current_display} ({self.temperature_monitoring_interval} seconds)\nMonitoring: {'Active' if self.temperature_monitoring_active else 'Inactive'}")
            msg_label.config(text=f"Temperature monitoring started ({current_display} interval)", fg="green")
        
        def stop_monitoring():
            if not self.temperature_monitoring_active:
                msg_label.config(text="Temperature monitoring is not active", fg="orange")
                return
            
            self.stop_temperature_monitoring()
            current_display = format_interval_display(self.temperature_monitoring_interval)
            status_label.config(text=f"Current Interval: {current_display} ({self.temperature_monitoring_interval} seconds)\nMonitoring: {'Active' if self.temperature_monitoring_active else 'Inactive'}")
            msg_label.config(text="Temperature monitoring stopped", fg="green")
        
        # Buttons frame
        button_frame = tk.Frame(popup)
        button_frame.pack(pady=10)
        
        apply_btn = tk.Button(button_frame, text="Apply", command=apply_settings, width=12, bg="lightblue")
        apply_btn.pack(side=tk.LEFT, padx=5)
        
        start_btn = tk.Button(button_frame, text="Start", command=start_monitoring, width=12, bg="lightgreen")
        start_btn.pack(side=tk.LEFT, padx=5)
        
        stop_btn = tk.Button(button_frame, text="Stop", command=stop_monitoring, width=12, bg="lightcoral")
        stop_btn.pack(side=tk.LEFT, padx=5)
        
        close_btn = tk.Button(button_frame, text="Close", command=popup.destroy, width=12)
        close_btn.pack(side=tk.LEFT, padx=5)
        
        # Bind Enter key to apply
        interval_entry.bind('<Return>', lambda e: apply_settings())
    
    def start_temperature_monitoring(self):
        """Start periodic temperature monitoring"""
        if self.temperature_monitoring_active:
            return  # Already monitoring
        
        if not self.clients:
            self.log("ERROR: Cannot start temperature monitoring - no devices connected")
            return
        
        self.temperature_monitoring_active = True
        # Format interval display for log and button
        interval_minutes = self.temperature_monitoring_interval // 60
        if interval_minutes >= 60:
            # Display in hours
            interval_hr = interval_minutes // 60
            interval_remainder_min = interval_minutes % 60
            if interval_remainder_min == 0:
                interval_display = f"{interval_hr}hr"
                interval_log = f"{interval_hr} hour{'s' if interval_hr > 1 else ''} ({self.temperature_monitoring_interval}s)"
            else:
                interval_display = f"{interval_hr}h{interval_remainder_min}min"
                interval_log = f"{interval_hr}h {interval_remainder_min}min ({self.temperature_monitoring_interval}s)"
        elif interval_minutes >= 1:
            # Display in minutes
            interval_display = f"{interval_minutes}min"
            interval_log = f"{interval_minutes} minute{'s' if interval_minutes > 1 else ''} ({self.temperature_monitoring_interval}s)"
        else:
            # Display in seconds (shouldn't happen with new validation, but keep for safety)
            interval_display = f"{self.temperature_monitoring_interval}s"
            interval_log = f"{self.temperature_monitoring_interval} seconds"
        
        self.log(f"Temperature monitoring started (interval: {interval_log})")
        
        # Update button text
        self.temperature_monitor_btn.config(text=f"Temp Monitor ({interval_display})", bg="green")
        
        # Send initial temperature request
        self.request_temperature_status_all_devices()
        
        # Schedule periodic requests
        self.schedule_next_temperature_request()
    
    def stop_temperature_monitoring(self):
        """Stop periodic temperature monitoring"""
        if not self.temperature_monitoring_active:
            return  # Not monitoring
        
        self.temperature_monitoring_active = False
        
        # Cancel scheduled job if exists
        if self.temperature_monitoring_job:
            self.root.after_cancel(self.temperature_monitoring_job)
            self.temperature_monitoring_job = None
        
        # Update button text
        self.temperature_monitor_btn.config(text="Temp Monitor (OFF)", bg="gray")
        
        self.log("Temperature monitoring stopped")
    
    def schedule_next_temperature_request(self):
        """Schedule the next temperature status request"""
        if not self.temperature_monitoring_active:
            return
        
        # Schedule next request
        self.temperature_monitoring_job = self.root.after(
            self.temperature_monitoring_interval * 1000,  # Convert seconds to milliseconds
            self.request_temperature_status_all_devices
        )
    
    def request_temperature_status_all_devices(self):
        """Request temperature status from all connected devices"""
        if not self.temperature_monitoring_active:
            return
        
        if not self.clients:
            self.stop_temperature_monitoring()
            return
        
        # Send "temp amb" command to all connected devices (ambient temperature only)
        clients_copy = dict(self.clients)
        for address, client_info in clients_copy.items():
            try:
                write_char_uuid = client_info.get('write_char_uuid')
                if write_char_uuid:
                    device_name = client_info['name']
                    if device_name == address:
                        device_display = f"MAC:{address}"
                    else:
                        device_display = f"{device_name}"
                    
                    # Send temperature command asynchronously
                    async def send_temp_command(addr, uuid, display):
                        try:
                            client = self.clients[addr]['client']
                            if addr in self.clients:  # Double check device still connected
                                await client.write_gatt_char(uuid, b"temp amb")
                                # Don't log every request to reduce spam
                        except Exception as e:
                            self.log(f"Error requesting temperature status from {display}: {e}")
                            # Remove device if connection lost
                            if addr in self.clients:
                                del self.clients[addr]
                                self.update_connected_listbox()
                    
                    threading.Thread(
                        target=lambda: asyncio.run(send_temp_command(address, write_char_uuid, device_display)),
                        daemon=True
                    ).start()
            except Exception as e:
                self.log(f"Error preparing temperature request for {address}: {e}")
        
        # Schedule next request
        self.schedule_next_temperature_request()

def center_window(win, width=None, height=None):
    win.update_idletasks()
    if width is None or height is None:
        width = win.winfo_width()
        height = win.winfo_height()
        if width == 1: width = 800  # fallback default
        if height == 1: height = 600
    screen_width = win.winfo_screenwidth()
    screen_height = win.winfo_screenheight()
    x = (screen_width // 2) - (width // 2)
    y = (screen_height // 2) - (height // 2)
    win.geometry(f'{width}x{height}+{x}+{y}')

if __name__ == "__main__":

    root = tk.Tk()
    root.geometry("1280x800")  # Initial size
    root.minsize(1280, 600)     # Minimum size
    root.resizable(True, True) # Allow resizing
    
    # Set application icon
    try:
        icon_path = os.path.join(os.path.dirname(__file__), "inphys.ico")
        if os.path.exists(icon_path):
            root.iconbitmap(icon_path)
    except Exception as e:
        print(f"Warning: Could not load icon: {e}")
    
    app = RDCScannerApp(root)
    root.mainloop()

