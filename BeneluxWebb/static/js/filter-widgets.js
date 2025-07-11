/**
 * Filter Widgets - Reusable filter components for SmartTable
 * 
 * This module provides classes for custom filter widgets that can be used
 * with SmartTable to create consistent, reusable filtering UI components.
 */

/**
 * Base FilterWidget class
 */
class FilterWidget {
  constructor(options) {
    this.options = options;
    this.table = options.table;
  }

  init() {}
  getValue() { return null; }
  setValue(_) {}
  destroy() {}
}

/**
 * RangeSlider - Handles ELO or numeric range filters
 */
export class RangeSlider extends FilterWidget {
  constructor(options) {
    super(options);
    this.container = options.container;
    this.minName = options.minName || 'min';
    this.maxName = options.maxName || 'max';
    this.minValue = options.minValue || 0;
    this.maxValue = options.maxValue || 100;
    this.step = options.step || 1;

    this.minDisplay = options.minDisplay;
    this.maxDisplay = options.maxDisplay;

    this.minInput = options.minInput || this._createHiddenInput(this.minName);
    this.maxInput = options.maxInput || this._createHiddenInput(this.maxName);

    this.init();
  }

  _createHiddenInput(name) {
    const input = document.createElement('input');
    input.type = 'hidden';
    input.name = name;
    this.container.parentNode.appendChild(input);
    return input;
  }

  init() {
    if (!window.noUiSlider) {
      console.error('noUiSlider not loaded');
      return;
    }

    noUiSlider.create(this.container, {
      start: [this.minValue, this.maxValue],
      connect: true,
      range: { min: this.minValue, max: this.maxValue },
      step: this.step,
    });

    // Restore saved state values here
    const savedFilters = this.table?.filters || {};
    const min = savedFilters[this.minName] ?? this.minValue;
    const max = savedFilters[this.maxName] ?? this.maxValue;

    this.setValue([min, max]);

    // Update hidden inputs and displays
    this.minInput.value = min;
    this.maxInput.value = max;
    if (this.minDisplay) this.minDisplay.textContent = min;
    if (this.maxDisplay) this.maxDisplay.textContent = max;

    this.container.noUiSlider.on('update', ([minVal, maxVal]) => {
      minVal = Math.round(minVal);
      maxVal = Math.round(maxVal);
      if (this.minDisplay) this.minDisplay.textContent = minVal;
      if (this.maxDisplay) this.maxDisplay.textContent = maxVal;
    });

    this.container.noUiSlider.on('change', ([minVal, maxVal]) => {
      minVal = Math.round(minVal);
      maxVal = Math.round(maxVal);
      this.minInput.value = minVal;
      this.maxInput.value = maxVal;

      this.minInput.dispatchEvent(new Event('change', { bubbles: true }));
      this.maxInput.dispatchEvent(new Event('change', { bubbles: true }));

      console.log(`Range slider changed: ${minVal}-${maxVal}`);
    });
  }

  getValue() {
    return [parseInt(this.minInput.value), parseInt(this.maxInput.value)];
  }

  setValue([min, max]) {
    this.container.noUiSlider.set([min, max]);
  }

  destroy() {
    if (this.container.noUiSlider) {
      this.container.noUiSlider.destroy();
    }
  }
}

/**
 * CountryFilter - Checkbox group filter
 */
export class CountryFilter extends FilterWidget {
  constructor(options) {
    super(options);
    this.checkboxSelector = options.checkboxSelector || 'input[name="countries"]';
    this.name = options.name || 'countries';
    this.checkboxes = document.querySelectorAll(this.checkboxSelector);
    this.init();
  }

  init() {
    const saved = this.table?.filters?.[this.name];
    if (saved) {
      this.checkboxes.forEach(cb => {
        cb.checked = Array.isArray(saved) ? saved.includes(cb.value) : saved === cb.value;
      });
    }

    this.checkboxes.forEach(cb => {
      cb.addEventListener('change', () => {
        const checked = Array.from(
          document.querySelectorAll(this.checkboxSelector + ':checked')
        ).map(cb => cb.value);

        if (this.table) {
          this.table.filters[this.name] = checked;
          this.table.filters.page = 1;
          this.table.saveState();
          this.table.fetchData();
        }
      });
    });
  }

  getValue() {
    return Array.from(
      document.querySelectorAll(this.checkboxSelector + ':checked')
    ).map(cb => cb.value);
  }

  setValue(values) {
    if (!Array.isArray(values)) return;
    this.checkboxes.forEach(cb => {
      cb.checked = values.includes(cb.value);
    });
  }
}


/**
 * TomSelectFilter - for multi-select dropdowns
 */
export class TomSelectFilter extends FilterWidget {
  constructor(options) {
    super(options);
    this.selector = options.selector;
    this.filterName = options.filterName;
    this.filterState = options.filterState || null;
    this.tomSelectOptions = options.tomSelectOptions || {};

    this.selectElement = document.querySelector(this.selector);
    if (!this.selectElement) {
      console.warn(`TomSelectFilter: No element found for ${this.selector}`);
      return;
    }

    if (this.selectElement.tomselect) {
      this.selectElement.tomselect.destroy();
    }

    const defaultOptions = {
      plugins: ['remove_button', 'no_backspace_delete', 'no_active_items'],
      persist: false,
      create: true,
    };

    this.tsInstance = new TomSelect(this.selectElement, {
      ...defaultOptions,
      ...this.tomSelectOptions,
    });

    this.tsInstance.on('change', value => {
      const values = value ? value.split(',') : [];
      this.table.filters[this.filterName] = values;
      this.table.filters.page = 1;
      this.table.saveState();
      this.table.fetchData();

      if (this.filterState) {
        const saved = JSON.parse(localStorage.getItem(this.filterState.storageKey)) || {};
        saved[this.filterName] = values;
        localStorage.setItem(this.filterState.storageKey, JSON.stringify(saved));
      }
    });
  }

  init() {
    // Ensure TomSelect is fully initialized before restoring state
    setTimeout(() => {
      let values = [];

      // Restore from table filters if available
      if (this.table.filters?.[this.filterName]) {
        values = this.table.filters[this.filterName];
      }

      // Restore from localStorage if available
      if (this.filterState) {
        const saved = JSON.parse(localStorage.getItem(this.filterState.storageKey)) || {};
        if (Array.isArray(saved[this.filterName])) {
          values = saved[this.filterName];
        }
      }

      // Ensure values are an array
      if (typeof values === 'string') {
        values = values.length ? values.split(',') : [];
      }

      // Update table filters and set TomSelect value
      this.table.filters[this.filterName] = values;
      this.tsInstance.setValue(values, true);
    }, 0); // Use a minimal delay to ensure DOM readiness
  }

  getValue() {
    return this.tsInstance.getValue();
  }

  setValue(values) {
    this.tsInstance.setValue(values);
  }

  destroy() {
    this.tsInstance.destroy();
  }
}

/**
 * MultiCheckboxFilter - Handles multi-checkbox filters
 */
export class MultiCheckboxFilter extends FilterWidget {
  constructor(options) {
    super(options);
    this.checkboxSelector = options.checkboxSelector || 'input[type="checkbox"]';
    this.filterName = options.filterName || 'multiCheckbox';
    this.filterState = options.filterState || null;
    this.checkboxes = document.querySelectorAll(this.checkboxSelector);
    this.resetButtonSelector = options.resetButtonSelector || null;
    this.onCheckboxChange = this.updateFilter.bind(this);
    this.onResetClick = this.resetFilter.bind(this);
    this.toggleInputId = options.toggleInputId || 'toggle-check';
    this.dropdownWrapperSelector = options.dropdownWrapperSelector || '.dropdown-wrapper';
    this._onDocumentClick = this._handleOutsideClick.bind(this);
    this.init();
  }

  init() {
    // Restore saved state values
    const saved = this.table?.filters?.[this.filterName] || [];
    if (saved.length > 0) {
      this.setValue(saved);
    }

    // Add event listeners to checkboxes
    this.checkboxes.forEach(cb => {
      cb.addEventListener('change', this.onCheckboxChange);
    });

    if (this.resetButtonSelector) {
      const resetButton = document.querySelector(this.resetButtonSelector);
      if (resetButton) {
        resetButton.addEventListener('click', this.onResetClick);
      }
    }

    // Handle outside clicks to close dropdown
    document.addEventListener('click', this._onDocumentClick);
  }

  _handleOutsideClick(event) {
    const wrapper = document.querySelector(this.dropdownWrapperSelector);
    const toggleInput = document.getElementById(this.toggleInputId);

    if (!wrapper || !toggleInput) return;

    if (!wrapper.contains(event.target) && toggleInput.checked) {
      toggleInput.checked = false;
    }
  }
  updateFilter() {
    const checkedValues = Array.from(
      document.querySelectorAll(this.checkboxSelector + ':checked')
    ).map(cb => cb.value);

    // Update table filters and save state
    if (this.table) {
      this.table.filters[this.filterName] = checkedValues;
      this.table.filters.page = 1; // Reset page on filter change
      this.table.saveState();
      this.table.fetchData();
    }

    // Update filter state in localStorage
    if (this.filterState) {
      const saved = JSON.parse(localStorage.getItem(this.filterState.storageKey)) || {};
      saved[this.filterName] = checkedValues;
      localStorage.setItem(this.filterState.storageKey, JSON.stringify(saved));
    }
  }

  resetFilter() {
    // Uncheck all checkboxes
    this.checkboxes.forEach(cb => {
      cb.checked = false;
    });

    // Clear filter state
    if (this.table) {
      this.table.filters[this.filterName] = [];
      this.table.filters.page = 1;
      this.table.saveState();
      this.table.fetchData();
    }

    if (this.filterState) {
      const saved = JSON.parse(localStorage.getItem(this.filterState.storageKey)) || {};
      saved[this.filterName] = [];
      localStorage.setItem(this.filterState.storageKey, JSON.stringify(saved));
    }
  }

  getValue() {
    return Array.from(
      document.querySelectorAll(this.checkboxSelector + ':checked')
    ).map(cb => cb.value);
  }

  setValue(values) {
    if (!Array.isArray(values)) return;
    this.checkboxes.forEach(cb => {
      cb.checked = values.includes(cb.value);
    });
  }

  destroy() {
    this.checkboxes.forEach(cb => {
      cb.removeEventListener('change', this.onCheckboxChange);
    });

    const resetButton = document.querySelector(this.resetButtonSelector);
    if (resetButton) {
      resetButton.removeEventListener('click', this.onResetClick);
    }

    document.removeEventListener('click', this._onDocumentClick);
  }
}

/**
 * Register multiple filter widgets with SmartTable
 */
export function registerFilterWidgets(table, widgets) {
  if (!table || !table.filters) {
    console.error('Invalid SmartTable instance');
    return;
  }

  if (!Array.isArray(widgets)) widgets = [widgets];
  table.filterWidgets = table.filterWidgets || {};

  widgets.forEach(widget => {
    if (!widget || typeof widget.init !== 'function') return;
    table.filterWidgets[widget.constructor.name] = widget;
  });

  console.log('Filter widgets registered:', table.filterWidgets);
}