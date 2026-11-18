(() => {
  const key = 'rivomesh-theme';
  const root = document.documentElement;
  const systemTheme = window.matchMedia('(prefers-color-scheme: dark)');
  let preferred = null;
  let toggle;

  try {
    preferred = localStorage.getItem(key);
  } catch {
    // An unavailable preference store does not affect the console session.
  }

  function renderTheme() {
    const dark = preferred === 'dark' || (preferred !== 'light' && systemTheme.matches);
    root.dataset.theme = dark ? 'dark' : 'light';
    toggle?.setAttribute('aria-pressed', String(dark));
  }

  renderTheme();
  systemTheme.addEventListener('change', renderTheme);
  window.addEventListener('storage', event => {
    if (event.key === key || event.key === null) {
      preferred = event.newValue;
      renderTheme();
    }
  });

  document.addEventListener('DOMContentLoaded', () => {
    toggle = document.getElementById('theme-toggle');
    toggle.classList.remove('hidden');
    renderTheme();
    toggle.addEventListener('click', () => {
      preferred = root.dataset.theme === 'dark' ? 'light' : 'dark';
      try {
        localStorage.setItem(key, preferred);
      } catch {
        // Keep the change for this page.
      }
      renderTheme();
    });
  }, {once: true});
})();
