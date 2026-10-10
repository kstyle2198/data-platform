
import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Data Platform Monitor",
  description: "Kafka, Spark, Iceberg and MinIO monitoring dashboard"
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="ko">
      <body>{children}</body>
    </html>
  );
}
