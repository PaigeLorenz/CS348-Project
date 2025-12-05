"""
Simple record tracker GUI using tkinter and SQLite

Features:
- Initializes SQLite database (records.db) and tables (Artists, Records)
- Main table view lists records
- Add Record dialog (creates artist if missing)
- Edit Record dialog
- Delete Record (optional: remove artist if no records remain)

Run:
    python gui_records.py

No external dependencies (uses Python's built-in sqlite3 and tkinter).
"""

import sqlite3
import threading
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from datetime import datetime
from typing import Optional

# HTTP API client
import requests
API_HOST = '127.0.0.1'
API_PORT = 5000
API_BASE = f'http://{API_HOST}:{API_PORT}/api'

# import DB helpers and server starter from separate module
from server import (
    init_db,
    fetch_all_records_db,
    add_record_db,
    update_record_db,
    delete_record_db,
    get_artists_db,
    get_genres_db,
    get_stores_db,
    find_or_create_artist,
    find_or_create_genre,
    find_or_create_store,
    start_api_in_thread,
)






# --- API client helpers (GUI uses these) ---
def api_fetch_all_records():
    try:
        resp = requests.get(f"{API_BASE}/records", timeout=2)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        # fallback to DB direct if API not available
        rows = fetch_all_records_db()
    cols = ['record_id', 'title', 'artist', 'genre', 'store', 'year', 'condition', 'price', 'purchase_date']
    return [dict(zip(cols, r)) for r in rows]


def api_add_record(payload: dict):
    resp = requests.post(f"{API_BASE}/records", json=payload)
    resp.raise_for_status()
    return resp.json().get('record_id')


def api_update_record(rid: int, payload: dict):
    resp = requests.put(f"{API_BASE}/records/{rid}", json=payload)
    resp.raise_for_status()


def api_delete_record(rid: int):
    resp = requests.delete(f"{API_BASE}/records/{rid}")
    resp.raise_for_status()


def api_get_artists():
    try:
        resp = requests.get(f"{API_BASE}/artists", timeout=2)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return [{'artist_id': r[0], 'name': r[1]} for r in get_artists_db()]


def api_create_artist(name: str, country: str = None):
    payload = {'name': name}
    if country:
        payload['country'] = country
    resp = requests.post(f"{API_BASE}/artists", json=payload)
    resp.raise_for_status()
    return resp.json().get('artist_id')


def api_update_artist(aid: int, name: str, country: str = None):
    payload = {'name': name}
    if country is not None:
        payload['country'] = country
    resp = requests.put(f"{API_BASE}/artists/{aid}", json=payload)
    resp.raise_for_status()


def api_delete_artist(aid: int):
    resp = requests.delete(f"{API_BASE}/artists/{aid}")
    if resp.status_code == 400:
        raise Exception(resp.json().get('error'))
    resp.raise_for_status()


def api_get_genres():
    try:
        resp = requests.get(f"{API_BASE}/genres", timeout=2)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return [{'genre_id': r[0], 'name': r[1]} for r in get_genres_db()]


def api_create_genre(name: str):
    resp = requests.post(f"{API_BASE}/genres", json={'name': name})
    resp.raise_for_status()
    return resp.json().get('genre_id')


def api_update_genre(gid: int, name: str):
    resp = requests.put(f"{API_BASE}/genres/{gid}", json={'name': name})
    resp.raise_for_status()


def api_delete_genre(gid: int):
    resp = requests.delete(f"{API_BASE}/genres/{gid}")
    if resp.status_code == 400:
        raise Exception(resp.json().get('error'))
    resp.raise_for_status()


def api_get_stores():
    try:
        resp = requests.get(f"{API_BASE}/stores", timeout=2)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return [{'store_id': r[0], 'name': r[1], 'state': r[2], 'address': r[3]} for r in get_stores_db()]


def api_create_store(name: str, state: str = None, address: str = None):
    payload = {'name': name}
    if state:
        payload['state'] = state
    if address:
        payload['address'] = address
    resp = requests.post(f"{API_BASE}/stores", json=payload)
    resp.raise_for_status()
    return resp.json().get('store_id')


def api_update_store(sid: int, name: str, state: str = None, address: str = None):
    payload = {'name': name}
    if state is not None:
        payload['state'] = state
    if address is not None:
        payload['address'] = address
    resp = requests.put(f"{API_BASE}/stores/{sid}", json=payload)
    resp.raise_for_status()


def api_delete_store(sid: int):
    resp = requests.delete(f"{API_BASE}/stores/{sid}")
    resp.raise_for_status()



def api_report_records(filters: dict):
    resp = requests.post(f"{API_BASE}/reports/records", json=filters)
    resp.raise_for_status()
    return resp.json()




# GUI

class RecordsApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('Record Tracker')
        self.geometry('900x500')

        # Toolbar buttons
        toolbar = tk.Frame(self)
        toolbar.pack(side=tk.TOP, fill=tk.X, padx=8, pady=6)

        add_btn = tk.Button(toolbar, text='Add', command=self.on_add)
        add_btn.pack(side=tk.LEFT, padx=4)
        edit_btn = tk.Button(toolbar, text='Edit', command=self.on_edit)
        edit_btn.pack(side=tk.LEFT, padx=4)
        delete_btn = tk.Button(toolbar, text='Delete', command=self.on_delete)
        delete_btn.pack(side=tk.LEFT, padx=4)
        refresh_btn = tk.Button(toolbar, text='Refresh', command=self.load_records)
        refresh_btn.pack(side=tk.LEFT, padx=4)
        report_btn = tk.Button(toolbar, text='Report', command=self.on_report)
        report_btn.pack(side=tk.LEFT, padx=4)

        # Notebook with tabs for Records, Artists, Genres, Stores
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # Records tab
        self.records_frame = tk.Frame(self.notebook)
        self.notebook.add(self.records_frame, text='Records')

        # place the main records tree inside records_frame
        cols = ('id', 'title', 'artist', 'genre', 'store', 'year', 'condition', 'price', 'purchase_date')
        self.tree = ttk.Treeview(self.records_frame, columns=cols, show='headings')
        self.tree.heading('id', text='ID')
        self.tree.column('id', width=50, anchor=tk.CENTER)
        self.tree.heading('title', text='Title')
        self.tree.column('title', width=200)
        self.tree.heading('artist', text='Artist')
        self.tree.column('artist', width=140)
        self.tree.heading('genre', text='Genre')
        self.tree.column('genre', width=100)
        self.tree.heading('store', text='Store')
        self.tree.column('store', width=140)
        self.tree.heading('year', text='Year')
        self.tree.column('year', width=60, anchor=tk.CENTER)
        self.tree.heading('condition', text='Condition')
        self.tree.column('condition', width=80)
        self.tree.heading('price', text='Price')
        self.tree.column('price', width=80, anchor=tk.E)
        self.tree.heading('purchase_date', text='Purchase Date')
        self.tree.column('purchase_date', width=120)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        self.tree.bind('<Double-1>', lambda e: self.on_edit())

        # Artists tab
        self.artists_frame = tk.Frame(self.notebook)
        self.notebook.add(self.artists_frame, text='Artists')
        self._init_artists_tab()

        # Genres tab
        self.genres_frame = tk.Frame(self.notebook)
        self.notebook.add(self.genres_frame, text='Genres')
        self._init_genres_tab()

        # Stores tab
        self.stores_frame = tk.Frame(self.notebook)
        self.notebook.add(self.stores_frame, text='Stores')
        self._init_stores_tab()

        # load initial data
        self.load_records()
        self.load_artists()
        self.load_genres()
        self.load_stores()

    def load_records(self):
        for r in self.tree.get_children():
            self.tree.delete(r)
        rows = api_fetch_all_records()
        for r in rows:
            vals = (
                r.get('record_id'),
                r.get('title'),
                r.get('artist'),
                r.get('genre'),
                r.get('store'),
                r.get('year'),
                r.get('condition'),
                r.get('price'),
                r.get('purchase_date')
            )
            self.tree.insert('', tk.END, values=vals)

    def get_selected_record(self) -> Optional[int]:
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo('Select a record', 'Please select a record first.')
            return None
        item = sel[0]
        values = self.tree.item(item, 'values')
        return int(values[0])

    def on_add(self):
        dlg = RecordDialog(self, title='Add Record')
        self.wait_window(dlg)
        if dlg.result:
            data = dlg.result
            # map store name -> id
            store_name = data.get('store_name')
            store_id = None
            if store_name:
                stores = api_get_stores()
                s = next((x for x in stores if x['name'] == store_name), None)
                store_id = s['store_id'] if s else None
            payload = {
                'title': data.get('title'),
                'artist_name': data.get('artist_name'),
                'genre': data.get('genre'),
                'year': data.get('year'),
                'condition': data.get('condition'),
                'price': data.get('price'),
                'purchase_date': data.get('purchase_date'),
                'store_id': store_id
            }
            try:
                api_add_record(payload)
                messagebox.showinfo('Added', 'Record added successfully.')
                self.load_records()
            except Exception as e:
                messagebox.showerror('Error', f'Failed to add record: {e}')

    def on_edit(self):
        rid = self.get_selected_record()
        if not rid:
            return
        # fetch record via API
        rows = api_fetch_all_records()
        row = next((x for x in rows if int(x.get('record_id')) == int(rid)), None)
        if not row:
            messagebox.showerror('Error', 'Record not found')
            return
        initial = {
            'title': row.get('title', ''),
            'artist_name': row.get('artist') or '',
            'genre': row.get('genre') or '',
            'store': row.get('store') or '',
            'year': row.get('year') or '',
            'condition': row.get('condition') or '',
            'price': row.get('price') or '',
            'purchase_date': row.get('purchase_date') or ''
        }
        dlg = RecordDialog(self, title='Edit Record', initial=initial)
        self.wait_window(dlg)
        if dlg.result:
            data = dlg.result
            store_name = data.get('store_name')
            store_id = None
            if store_name:
                stores = api_get_stores()
                s = next((x for x in stores if x['name'] == store_name), None)
                store_id = s['store_id'] if s else None
            payload = {
                'title': data.get('title'),
                'artist_name': data.get('artist_name'),
                'genre': data.get('genre'),
                'year': data.get('year'),
                'condition': data.get('condition'),
                'price': data.get('price'),
                'purchase_date': data.get('purchase_date'),
                'store_id': store_id
            }
            try:
                api_update_record(rid, payload)
                messagebox.showinfo('Updated', 'Record updated successfully.')
                self.load_records()
            except Exception as e:
                messagebox.showerror('Error', f'Failed to update record: {e}')

    def on_delete(self):
        rid = self.get_selected_record()
        if not rid:
            return
        if messagebox.askyesno('Confirm', 'Are you sure you want to delete this record?'):
            try:
                api_delete_record(rid)
                messagebox.showinfo('Deleted', 'Record deleted successfully.')
                self.load_records()
            except Exception as e:
                messagebox.showerror('Error', f'Failed to delete record: {e}')

    def on_report(self):
        dlg = ReportDialog(self)
        self.wait_window(dlg)
        # Report dialog handles its own display

    # --- Artists / Genres / Stores tabs helpers ---
    def _init_artists_tab(self):
        top = tk.Frame(self.artists_frame)
        top.pack(fill=tk.X, padx=6, pady=6)
        tk.Button(top, text='Add Artist', command=self.add_artist).pack(side=tk.LEFT, padx=4)
        tk.Button(top, text='Edit Artist', command=self.edit_artist).pack(side=tk.LEFT, padx=4)
        tk.Button(top, text='Delete Artist', command=self.delete_artist).pack(side=tk.LEFT, padx=4)
        tk.Button(top, text='Refresh', command=self.load_artists).pack(side=tk.LEFT, padx=4)
        cols = ('id', 'name', 'country')
        self.artists_tree = ttk.Treeview(self.artists_frame, columns=cols, show='headings')
        for c, w in (('id', 50), ('name', 200), ('country', 120)):
            self.artists_tree.heading(c, text=c.title())
            self.artists_tree.column(c, width=w)
        self.artists_tree.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

    def load_artists(self):
        try:
            rows = api_get_artists()
        except Exception:
            rows = []
        for r in getattr(self, 'artists_tree').get_children():
            self.artists_tree.delete(r)
        for a in rows:
            self.artists_tree.insert('', tk.END, values=(a.get('artist_id'), a.get('name'), a.get('country') if 'country' in a else None))

    def get_selected_artist(self) -> Optional[int]:
        sel = self.artists_tree.selection()
        if not sel:
            messagebox.showinfo('Select an artist', 'Please select an artist first.')
            return None
        item = sel[0]
        values = self.artists_tree.item(item, 'values')
        return int(values[0])

    def edit_artist(self):
        aid = self.get_selected_artist()
        if not aid:
            return
        # get current values
        item = self.artists_tree.selection()[0]
        vals = self.artists_tree.item(item, 'values')
        cur_name = vals[1]
        cur_country = vals[2] if len(vals) > 2 else ''
        name = simpledialog.askstring('Edit artist', 'Artist name:', initialvalue=cur_name, parent=self)
        if not name:
            return
        country = simpledialog.askstring('Artist country', 'Country (optional):', initialvalue=cur_country, parent=self)
        try:
            api_update_artist(aid, name, country)
            self.load_artists()
            self.load_records()
        except Exception as e:
            messagebox.showerror('Error', f'Failed to update artist: {e}')

    def delete_artist(self):
        aid = self.get_selected_artist()
        if not aid:
            return
        if not messagebox.askyesno('Confirm', 'Delete selected artist?'):
            return
        try:
            api_delete_artist(aid)
            self.load_artists()
            self.load_records()
        except Exception as e:
            messagebox.showerror('Error', f'Failed to delete artist: {e}')

    def add_artist(self):
        name = simpledialog.askstring('New artist', 'Artist name:', parent=self)
        if not name:
            return
        country = simpledialog.askstring('Artist country', 'Country (optional):', parent=self)
        try:
            api_create_artist(name, country=country)
            self.load_artists()
        except Exception as e:
            messagebox.showerror('Error', f'Failed to add artist: {e}')

    def _init_genres_tab(self):
        top = tk.Frame(self.genres_frame)
        top.pack(fill=tk.X, padx=6, pady=6)
        tk.Button(top, text='Add Genre', command=self.add_genre).pack(side=tk.LEFT, padx=4)
        tk.Button(top, text='Edit Genre', command=self.edit_genre).pack(side=tk.LEFT, padx=4)
        tk.Button(top, text='Delete Genre', command=self.delete_genre).pack(side=tk.LEFT, padx=4)
        tk.Button(top, text='Refresh', command=self.load_genres).pack(side=tk.LEFT, padx=4)
        cols = ('id', 'name')
        self.genres_tree = ttk.Treeview(self.genres_frame, columns=cols, show='headings')
        for c, w in (('id', 50), ('name', 200)):
            self.genres_tree.heading(c, text=c.title())
            self.genres_tree.column(c, width=w)
        self.genres_tree.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

    def load_genres(self):
        try:
            rows = api_get_genres()
        except Exception:
            rows = []
        for r in getattr(self, 'genres_tree').get_children():
            self.genres_tree.delete(r)
        for g in rows:
            self.genres_tree.insert('', tk.END, values=(g.get('genre_id'), g.get('name')))

    def get_selected_genre(self) -> Optional[int]:
        sel = self.genres_tree.selection()
        if not sel:
            messagebox.showinfo('Select a genre', 'Please select a genre first.')
            return None
        item = sel[0]
        values = self.genres_tree.item(item, 'values')
        return int(values[0])

    def edit_genre(self):
        gid = self.get_selected_genre()
        if not gid:
            return
        item = self.genres_tree.selection()[0]
        vals = self.genres_tree.item(item, 'values')
        cur_name = vals[1]
        name = simpledialog.askstring('Edit genre', 'Genre name:', initialvalue=cur_name, parent=self)
        if not name:
            return
        try:
            api_update_genre(gid, name)
            self.load_genres()
            self.load_records()
        except Exception as e:
            messagebox.showerror('Error', f'Failed to update genre: {e}')

    def delete_genre(self):
        gid = self.get_selected_genre()
        if not gid:
            return
        if not messagebox.askyesno('Confirm', 'Delete selected genre?'):
            return
        try:
            api_delete_genre(gid)
            self.load_genres()
            self.load_records()
        except Exception as e:
            messagebox.showerror('Error', f'Failed to delete genre: {e}')

    def add_genre(self):
        name = simpledialog.askstring('New genre', 'Genre name:', parent=self)
        if not name:
            return
        try:
            api_create_genre(name)
            self.load_genres()
        except Exception as e:
            messagebox.showerror('Error', f'Failed to add genre: {e}')

    def _init_stores_tab(self):
        top = tk.Frame(self.stores_frame)
        top.pack(fill=tk.X, padx=6, pady=6)
        tk.Button(top, text='Add Store', command=self.add_store).pack(side=tk.LEFT, padx=4)
        tk.Button(top, text='Edit Store', command=self.edit_store).pack(side=tk.LEFT, padx=4)
        tk.Button(top, text='Delete Store', command=self.delete_store).pack(side=tk.LEFT, padx=4)
        tk.Button(top, text='Refresh', command=self.load_stores).pack(side=tk.LEFT, padx=4)
        cols = ('id', 'name', 'state', 'address')
        self.stores_tree = ttk.Treeview(self.stores_frame, columns=cols, show='headings')
        for c, w in (('id', 50), ('name', 180), ('state', 80), ('address', 200)):
            self.stores_tree.heading(c, text=c.title())
            self.stores_tree.column(c, width=w)
        self.stores_tree.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

    def load_stores(self):
        try:
            rows = api_get_stores()
        except Exception:
            rows = []
        for r in getattr(self, 'stores_tree').get_children():
            self.stores_tree.delete(r)
        for s in rows:
            self.stores_tree.insert('', tk.END, values=(s.get('store_id'), s.get('name'), s.get('state'), s.get('address')))

    def get_selected_store(self) -> Optional[int]:
        sel = self.stores_tree.selection()
        if not sel:
            messagebox.showinfo('Select a store', 'Please select a store first.')
            return None
        item = sel[0]
        values = self.stores_tree.item(item, 'values')
        return int(values[0])

    def edit_store(self):
        sid = self.get_selected_store()
        if not sid:
            return
        item = self.stores_tree.selection()[0]
        vals = self.stores_tree.item(item, 'values')
        cur_name = vals[1]
        cur_state = vals[2] if len(vals) > 2 else ''
        cur_address = vals[3] if len(vals) > 3 else ''
        name = simpledialog.askstring('Edit store', 'Store name:', initialvalue=cur_name, parent=self)
        if not name:
            return
        state = simpledialog.askstring('Store state', 'State (optional):', initialvalue=cur_state, parent=self)
        address = simpledialog.askstring('Store address', 'Address (optional):', initialvalue=cur_address, parent=self)
        try:
            api_update_store(sid, name, state=state, address=address)
            self.load_stores()
            self.load_records()
        except Exception as e:
            messagebox.showerror('Error', f'Failed to update store: {e}')

    def delete_store(self):
        sid = self.get_selected_store()
        if not sid:
            return
        if not messagebox.askyesno('Confirm', 'Delete selected store? This will remove links to records.'):
            return
        try:
            api_delete_store(sid)
            self.load_stores()
            self.load_records()
        except Exception as e:
            messagebox.showerror('Error', f'Failed to delete store: {e}')

    def add_store(self):
        name = simpledialog.askstring('New store', 'Store name:', parent=self)
        if not name:
            return
        state = simpledialog.askstring('Store state', 'State (optional):', parent=self)
        address = simpledialog.askstring('Store address', 'Address (optional):', parent=self)
        try:
            api_create_store(name, state=state, address=address)
            self.load_stores()
        except Exception as e:
            messagebox.showerror('Error', f'Failed to add store: {e}')


class RecordDialog(tk.Toplevel):
    def __init__(self, parent, title='Record', initial=None):
        super().__init__(parent)
        self.title(title)
        self.transient(parent)
        self.grab_set()
        self.result = None

        self.initial = initial or {}

        frm = tk.Frame(self)
        frm.pack(padx=12, pady=12, fill=tk.BOTH, expand=True)

        # Title
        tk.Label(frm, text='Title:').grid(row=0, column=0, sticky=tk.W)
        self.title_var = tk.StringVar(value=self.initial.get('title', ''))
        tk.Entry(frm, textvariable=self.title_var, width=50).grid(row=0, column=1, columnspan=2, sticky=tk.W)

        # Artist (combobox with add-new)
        tk.Label(frm, text='Artist:').grid(row=1, column=0, sticky=tk.W)
        self.artist_var = tk.StringVar(value=self.initial.get('artist_name', ''))
        self.artist_combo = ttk.Combobox(frm, textvariable=self.artist_var, values=[a['name'] for a in api_get_artists()], width=47)
        self.artist_combo.grid(row=1, column=1, sticky=tk.W)
        add_artist_btn = tk.Button(frm, text='Add new artist', command=self.add_new_artist)
        add_artist_btn.grid(row=1, column=2, sticky=tk.W)

        # Genre
        tk.Label(frm, text='Genre:').grid(row=2, column=0, sticky=tk.W)
        self.genre_var = tk.StringVar(value=self.initial.get('genre', ''))
        # allow selecting existing genre or typing new one
        self.genre_combo = ttk.Combobox(frm, textvariable=self.genre_var, values=[g['name'] for g in api_get_genres()])
        self.genre_combo.grid(row=2, column=1, columnspan=1, sticky=tk.W)
        # optional: button to add new genre
        add_genre_btn = tk.Button(frm, text='Add new genre', command=self.add_new_genre)
        add_genre_btn.grid(row=2, column=2, sticky=tk.W)

        # Store (combobox with add-new)
        tk.Label(frm, text='Store:').grid(row=3, column=0, sticky=tk.W)
        self.store_var = tk.StringVar(value=self.initial.get('store', ''))
        self.store_combo = ttk.Combobox(frm, textvariable=self.store_var, values=[s['name'] for s in api_get_stores()], width=37)
        self.store_combo.grid(row=3, column=1, sticky=tk.W)
        add_store_btn = tk.Button(frm, text='Add new store', command=self.add_new_store)
        add_store_btn.grid(row=3, column=2, sticky=tk.W)

        # Year
        tk.Label(frm, text='Year:').grid(row=4, column=0, sticky=tk.W)
        self.year_var = tk.StringVar(value=self.initial.get('year', ''))
        tk.Entry(frm, textvariable=self.year_var).grid(row=4, column=1, sticky=tk.W)

        # Condition
        tk.Label(frm, text='Condition:').grid(row=5, column=0, sticky=tk.W)
        self.cond_var = tk.StringVar(value=self.initial.get('condition', ''))
        tk.Entry(frm, textvariable=self.cond_var).grid(row=5, column=1, sticky=tk.W)

        # Price
        tk.Label(frm, text='Price:').grid(row=6, column=0, sticky=tk.W)
        self.price_var = tk.StringVar(value=self.initial.get('price', ''))
        tk.Entry(frm, textvariable=self.price_var).grid(row=6, column=1, sticky=tk.W)

        # Purchase date
        tk.Label(frm, text='Purchase date (YYYY-MM-DD):').grid(row=7, column=0, sticky=tk.W)
        self.pdate_var = tk.StringVar(value=self.initial.get('purchase_date', ''))
        tk.Entry(frm, textvariable=self.pdate_var).grid(row=7, column=1, sticky=tk.W)

        # Buttons
        btn_fr = tk.Frame(frm)
        btn_fr.grid(row=8, column=0, columnspan=3, pady=(12,0))
        tk.Button(btn_fr, text='Cancel', command=self.on_cancel).pack(side=tk.RIGHT, padx=6)
        tk.Button(btn_fr, text='Save', command=self.on_save).pack(side=tk.RIGHT, padx=6)

    def add_new_store(self):
        name = simpledialog.askstring('New store', 'Store name:', parent=self)
        if not name:
            return
        state = simpledialog.askstring('Store state', 'State (optional):', parent=self)
        address = simpledialog.askstring('Store address', 'Address (optional):', parent=self)
        try:
            api_create_store(name, state=state, address=address)
            # refresh combobox values
            self.store_combo['values'] = [s['name'] for s in api_get_stores()]
            self.store_var.set(name)
        except Exception as e:
            messagebox.showerror('Error', f'Failed to add store: {e}')

    def on_save(self):
        title = self.title_var.get().strip()
        if not title:
            messagebox.showerror('Validation', 'Title is required')
            return
        # optional basic date validation
        pdate = self.pdate_var.get().strip()
        if pdate:
            try:
                datetime.strptime(pdate, '%Y-%m-%d')
            except Exception:
                messagebox.showerror('Validation', 'Purchase date must be YYYY-MM-DD')
                return
        self.result = {
            'title': title,
            'artist_name': self.artist_var.get().strip(),
            'genre': self.genre_var.get().strip(),
            'store_name': self.store_var.get().strip(),
            'year': self.year_var.get().strip(),
            'condition': self.cond_var.get().strip(),
            'price': self.price_var.get().strip(),
            'purchase_date': pdate
        }
        self.destroy()

    def add_new_artist(self):
        name = simpledialog.askstring('New artist', 'Artist name:', parent=self)
        if not name:
            return
        country = simpledialog.askstring('Artist country', 'Country (optional):', parent=self)
        try:
            api_create_artist(name, country=country)
            # refresh combobox values
            self.artist_combo['values'] = [a['name'] for a in api_get_artists()]
            self.artist_var.set(name)
        except Exception as e:
            messagebox.showerror('Error', f'Failed to add artist: {e}')

    def add_new_genre(self):
        name = simpledialog.askstring('New genre', 'Genre name:', parent=self)
        if name:
            try:
                api_create_genre(name)
                # refresh genre combobox values
                self.genre_combo['values'] = [g['name'] for g in api_get_genres()]
                self.genre_var.set(name)
            except Exception as e:
                messagebox.showerror('Error', f'Failed to add genre: {e}')

    def on_cancel(self):
        self.result = None
        self.destroy()


class ReportDialog(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title('Records Report')
        self.transient(parent)
        self.grab_set()

        frm = tk.Frame(self)
        frm.pack(padx=12, pady=12, fill=tk.BOTH, expand=True)

        # Artist filter
        tk.Label(frm, text='Artist:').grid(row=0, column=0, sticky=tk.W)
        self.artist_var = tk.StringVar()
        self.artist_combo = ttk.Combobox(frm, textvariable=self.artist_var, values=['(All)'] + [a['name'] for a in api_get_artists()], width=30)
        self.artist_combo.grid(row=0, column=1, sticky=tk.W)
        self.artist_combo.set('(All)')
        # Store filter
        tk.Label(frm, text='Store:').grid(row=1, column=0, sticky=tk.W)
        self.store_var = tk.StringVar()
        self.store_combo = ttk.Combobox(frm, textvariable=self.store_var, values=['(All)'] + [s['name'] for s in api_get_stores()], width=30)
        self.store_combo.grid(row=1, column=1, sticky=tk.W)
        self.store_combo.set('(All)')
    # Genre filter
        tk.Label(frm, text='Genre:').grid(row=2, column=0, sticky=tk.W)
        self.genre_var = tk.StringVar()
        self.genre_combo = ttk.Combobox(frm, textvariable=self.genre_var, values=['(All)'] + [g['name'] for g in api_get_genres()], width=30)
        self.genre_combo.grid(row=2, column=1, sticky=tk.W)
        self.genre_combo.set('(All)')

        # Date range
        tk.Label(frm, text='Start date (YYYY-MM-DD):').grid(row=3, column=0, sticky=tk.W)
        self.start_var = tk.StringVar()
        tk.Entry(frm, textvariable=self.start_var).grid(row=3, column=1, sticky=tk.W)
        tk.Label(frm, text='End date (YYYY-MM-DD):').grid(row=4, column=0, sticky=tk.W)
        self.end_var = tk.StringVar()
        tk.Entry(frm, textvariable=self.end_var).grid(row=4, column=1, sticky=tk.W)

        btn_fr = tk.Frame(frm)
        btn_fr.grid(row=5, column=0, columnspan=2, pady=(8,0))
        tk.Button(btn_fr, text='Generate', command=self.generate).pack(side=tk.LEFT, padx=6)
        tk.Button(btn_fr, text='Close', command=self.on_close).pack(side=tk.LEFT, padx=6)

        # Results area
        self.stats_label = tk.Label(frm, text='')
        self.stats_label.grid(row=6, column=0, columnspan=2, sticky=tk.W, pady=(8,0))

        self.results_tree = None

    def on_close(self):
        self.destroy()

    def generate(self):
        # build filters
        # map selected names to ids
        artist_name = self.artist_var.get()
        artist_id = None
        if artist_name and artist_name != '(All)':
            artists = api_get_artists()
            a = next((x for x in artists if x['name'] == artist_name), None)
            artist_id = a['artist_id'] if a else None
        store_name = self.store_var.get()
        store_id = None
        if store_name and store_name != '(All)':
            stores = api_get_stores()
            s = next((x for x in stores if x['name'] == store_name), None)
            store_id = s['store_id'] if s else None
        genre_name = self.genre_var.get()
        genre_id = None
        if genre_name and genre_name != '(All)':
            genres = api_get_genres()
            g = next((x for x in genres if x['name'] == genre_name), None)
            genre_id = g['genre_id'] if g else None

        filters = {
            'start_date': self.start_var.get() or None,
            'end_date': self.end_var.get() or None,
            'artist_id': artist_id,
            'store_id': store_id,
            'genre_id': genre_id
        }
        try:
            res = api_report_records(filters)
        except Exception as e:
            messagebox.showerror('Error', f'Failed to fetch report: {e}')
            return

        stats = res.get('stats', {})
        txt = f"Count: {stats.get('count',0)}  Avg price: {stats.get('avg_price')}  Min: {stats.get('min_price')}  Max: {stats.get('max_price')}  Avg year: {stats.get('avg_year')}"
        self.stats_label.config(text=txt)

        # show rows
        if self.results_tree:
            self.results_tree.destroy()
        cols = ('id', 'title', 'artist', 'genre', 'store', 'year', 'condition', 'price', 'purchase_date')
        self.results_tree = ttk.Treeview(self, columns=cols, show='headings')
        for c in cols:
            self.results_tree.heading(c, text=c.title())
        for r in res.get('rows', []):
            vals = (
                r.get('record_id'), r.get('title'), r.get('artist'), r.get('genre'), r.get('store'), r.get('year'), r.get('condition'), r.get('price'), r.get('purchase_date')
            )
            self.results_tree.insert('', tk.END, values=vals)
        self.results_tree.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

    


if __name__ == '__main__':
    init_db()
    try:
        start_api_in_thread(host=API_HOST, port=API_PORT)
    except Exception:
        pass
    app = RecordsApp()
    app.mainloop()
