import {redirect} from 'next/navigation';

interface AdminTenantDetailPageProps {
  params: Promise<{tenantId: string}>;
}

export default async function AdminTenantDetailPage({params}: AdminTenantDetailPageProps) {
  const resolved = await params;
  redirect(`/tenants/${encodeURIComponent(resolved.tenantId)}`);
}
