// M2/M3/M4 会逐步补齐真正的页面。这里只是占位，防止 404
export default function ComingSoon({ name }: { name: string }) {
  return (
    <div className="max-w-3xl mx-auto px-4 py-16 text-center">
      <div className="text-5xl mb-4">🚧</div>
      <h1 className="text-xl font-semibold text-gray-800">{name}</h1>
      <p className="mt-2 text-gray-500">这个页面还在后续里程碑里做</p>
    </div>
  );
}
