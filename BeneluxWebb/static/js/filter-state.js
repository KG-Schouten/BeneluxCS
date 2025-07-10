export class FilterState {
  constructor(storageKey, selectors) {
    this.storageKey = storageKey; // e.g. 'leaderboardFilters'
    this.selectors = selectors;

    this.form = document.querySelector(selectors.form);
    this.searchInput = document.querySelector(selectors.search);
    this.perPageSelect = document.querySelector(selectors.perPage);
    this.countryCheckboxes = document.querySelectorAll(selectors.countries);
    this.eloSlider = document.querySelector(selectors.eloSlider);
  }

  load() {
    const saved = JSON.parse(localStorage.getItem(this.storageKey)) || {};

    if (saved.search && this.searchInput) this.searchInput.value = saved.search;
    if (saved.per_page && this.perPageSelect) this.perPageSelect.value = saved.per_page;

    if (this.countryCheckboxes.length) {
      this.countryCheckboxes.forEach(cb => {
        cb.checked = saved.countries?.includes(cb.value) ?? true;
      });
    }

    if (saved.columns && this.form) {
      const columnCheckboxes = this.form.querySelectorAll('input[name="columns"]');
      columnCheckboxes.forEach(cb => {
        cb.checked = saved.columns.includes(cb.value);
      });
    }

    if (
      saved.min_elo !== undefined &&
      saved.max_elo !== undefined &&
      this.eloSlider?.noUiSlider
    ) {
      this.eloSlider.noUiSlider.set([saved.min_elo, saved.max_elo]);
    }

    if (this.form) {
      if (saved.sort) this.form.dataset.sort = saved.sort;
      if (saved.page && saved.page > 1) this.form.dataset.page = saved.page;
    }
  }

  save(extraFilters = {}) {
    const countries = Array.from(this.countryCheckboxes)
      .filter(cb => cb.checked)
      .map(cb => cb.value);

    let minElo = 0, maxElo = 5000;
    if (this.eloSlider?.noUiSlider) {
      [minElo, maxElo] = this.eloSlider.noUiSlider.get().map(v => Math.round(v));
    }

    const baseFilters = {
      search: this.searchInput?.value || '',
      min_elo: minElo,
      max_elo: maxElo,
      countries,
      per_page: this.perPageSelect?.value || '25',
      sort: this.form?.dataset.sort || '',
      page: this.form?.dataset.page || '1',
    };

    const combined = { ...baseFilters, ...extraFilters };
    localStorage.setItem(this.storageKey, JSON.stringify(combined));
  }

  getFilters() {
    let minElo = 0, maxElo = 5000;
    if (this.eloSlider?.noUiSlider) {
      [minElo, maxElo] = this.eloSlider.noUiSlider.get().map(v => Math.round(v));
    }

    const countries = Array.from(this.countryCheckboxes)
      .filter(cb => cb.checked)
      .map(cb => cb.value);

    return {
      search: this.searchInput?.value.trim() || '',
      min_elo: minElo,
      max_elo: maxElo,
      countries,
      per_page: this.perPageSelect?.value || '25',
      sort: this.form?.dataset.sort || '',
      page: this.form?.dataset.page || '1',
    };
  }

  setPage(pageNum) {
    if (this.form) {
      this.form.dataset.page = String(pageNum);
    }
  }
}