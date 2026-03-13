export interface ProductDefinition {
  id: 'memoria_basica' | 'comparacion_presupuestos';
  title: string;
  href: string;
}

const PRODUCTS: ProductDefinition[] = [
  {
    id: 'memoria_basica',
    title: 'Generación de memoria básica',
    href: '/products/memoria-basica',
  },
  {
    id: 'comparacion_presupuestos',
    title: 'Comparación de presupuestos',
    href: '/products/comparacion-presupuestos',
  },
];

const ALIAS_MAP: Record<string, ProductDefinition['id']> = {
  memoria_basica: 'memoria_basica',
  'memoria-basica': 'memoria_basica',
  memoria_basic: 'memoria_basica',
  comparacion_presupuestos: 'comparacion_presupuestos',
  'comparacion-presupuestos': 'comparacion_presupuestos',
  budget_comparison: 'comparacion_presupuestos',
  comparador_presupuestos: 'comparacion_presupuestos',
};

export function getProductDefinition(idOrAlias: string): ProductDefinition | null {
  const canonicalId = ALIAS_MAP[idOrAlias.toLowerCase()];

  if (!canonicalId) {
    return null;
  }

  return PRODUCTS.find((product) => product.id === canonicalId) ?? null;
}

export function resolveTenantProducts(productIds: string[]): ProductDefinition[] {
  const resolved: ProductDefinition[] = [];
  const seen = new Set<string>();

  for (const raw of productIds) {
    const def = getProductDefinition(raw);

    if (def && !seen.has(def.id)) {
      seen.add(def.id);
      resolved.push(def);
    }
  }

  return resolved;
}

export function getAllProducts(): ProductDefinition[] {
  return [...PRODUCTS];
}
