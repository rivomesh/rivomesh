import assert from 'node:assert/strict';

export async function verifyAppearance({cdp,evaluate,key,viewport,screenshot,waitFor,passed}) {
  async function systemTheme(value) {
    await cdp('Emulation.setEmulatedMedia',{features:[{name:'prefers-color-scheme',value}]});
    await waitFor(()=>evaluate(`document.documentElement.dataset.theme===${JSON.stringify(value)}`),
                  'theme follows system preference');
  }

  await viewport(1280,900);
  await systemTheme('light');
  const assets=await evaluate(`Promise.all(['/brand/wordmark.svg','/brand/wordmark-light.svg','/brand/favicon.svg'].map(
    url=>fetch(url).then(response=>({status:response.status,type:response.headers.get('content-type')}))))`);
  assert(assets.every(({status,type})=>status===200&&type==='image/svg+xml'));
  await waitFor(()=>evaluate("document.querySelector('.sidebar .brand-day').naturalWidth>0"),'packaged light wordmark');
  assert.equal(await evaluate("getComputedStyle(document.documentElement).getPropertyValue('--accent').trim()"),'#2859a6');
  await systemTheme('dark');
  assert.equal(await evaluate("getComputedStyle(document.documentElement).getPropertyValue('--accent').trim()"),'#94bcfa');
  assert.equal(await evaluate("getComputedStyle(document.querySelector('.sidebar')).backgroundColor"),'rgb(17, 28, 45)');
  await waitFor(()=>evaluate("document.querySelector('.sidebar .brand-night').naturalWidth>0"),'packaged dark wordmark');
  assert.equal(await evaluate("getComputedStyle(document.querySelector('.sidebar .brand-day')).display"),'none');
  const tree=await cdp('Accessibility.getFullAXTree');
  const themeButton=tree.nodes.find(node=>node.name?.value==='Dark theme'&&
    ['button','toggleButton'].includes(node.role?.value));
  assert(themeButton,'Dark theme button has an accessible name');
  assert(themeButton.properties.some(property=>property.name==='pressed'&&property.value.value==='true'));
  await evaluate('window.scrollTo(0,0)');
  await screenshot('workspace-dark');
  passed('Packaged site wordmarks, favicon and palette follow the browser color scheme');

  await evaluate('document.querySelector("#theme-toggle").focus()');
  await key('Enter',13);
  assert.equal(await evaluate("document.querySelector('#theme-toggle').getAttribute('aria-pressed')"),'false');
  await cdp('Page.reload');
  await waitFor(()=>evaluate('!!document.querySelector(".task-card") && !document.querySelector("#login-dialog").open'),
                'appearance preference reload retains the authenticated session');
  assert.equal(await evaluate('document.documentElement.dataset.theme'),'light');
  assert.deepEqual(await evaluate('Object.keys(localStorage).sort()'),['rivomesh-language','rivomesh-theme']);
  passed('An explicit keyboard-selected appearance survives reload with the authenticated cookie session');

  await viewport(320,640);
  await evaluate('document.querySelector(".skip-link").focus()');
  await key('Enter',13);
  assert.equal(await evaluate('document.activeElement.id'),'main-content');
  await evaluate('document.querySelector("#adapters-nav").focus()');
  await key('Enter',13);
  assert.equal(await evaluate('document.querySelector(".workspace-nav [aria-current=page]").id'),'adapters-nav');
  assert(await evaluate('document.documentElement.scrollWidth<=innerWidth'));
  assert(await evaluate("document.querySelector('#theme-toggle').getBoundingClientRect().height>=44"));
  await evaluate('document.querySelector("#tasks-nav").click();window.scrollTo(0,0)');
  assert.equal(await evaluate('document.querySelector(".workspace-nav [aria-current=page]").id'),'tasks-nav');
  assert(await evaluate('document.documentElement.scrollWidth<=innerWidth'));
  await screenshot('workspace-320px');
  passed('Keyboard skip and current-page navigation work at 320 CSS pixels with no page overflow');
}
